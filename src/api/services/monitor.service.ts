import { InstanceDto } from '@api/dto/instance.dto';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository } from '@api/repository/repository.service';
import { channelController } from '@api/server.module';
import { Events, Integration } from '@api/types/wa.types';
import { CacheConf, Chatwoot, ConfigService, Database, DelInstance, ProviderSession } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { INSTANCE_DIR, STORE_DIR } from '@config/path.config';
import { NotFoundException } from '@exceptions';
import { execFileSync } from 'child_process';
import EventEmitter2 from 'eventemitter2';
import { rmSync } from 'fs';
import { join } from 'path';

import { CacheService } from './cache.service';

type BootInstance = InstanceDto & { disconnectionReasonCode?: number | null };

export class WAMonitoringService {
  // Quedas passageiras: a sessão segue válida e a instância deve voltar no boot mesmo se o banco ficou 'close'.
  // Ficam de fora as definitivas (401 logout, 403, 402, 406) e o 'close' sem motivo (ex.: limite de QR).
  private static readonly TRANSIENT_DISCONNECT_CODES = [408, 428, 440, 500, 503, 515];
  private static readonly BOOT_CONNECT_CONCURRENCY = 20;
  private static readonly BOOT_CONNECT_TIMEOUT_MS = 30_000;

  constructor(
    private readonly eventEmitter: EventEmitter2,
    private readonly configService: ConfigService,
    private readonly prismaRepository: PrismaRepository,
    private readonly providerFiles: ProviderFiles,
    private readonly cache: CacheService,
    private readonly chatwootCache: CacheService,
    private readonly baileysCache: CacheService,
  ) {
    this.removeInstance();
    this.noConnection();

    Object.assign(this.db, configService.get<Database>('DATABASE'));
    Object.assign(this.redis, configService.get<CacheConf>('CACHE'));

    (this as any).providerSession = Object.freeze(configService.get<ProviderSession>('PROVIDER'));
  }

  private readonly db: Partial<Database> = {};
  private readonly redis: Partial<CacheConf> = {};

  private readonly logger = new Logger('WAMonitoringService');
  public readonly waInstances: Record<string, any> = {};
  private readonly delInstanceTimeouts: Record<string, NodeJS.Timeout> = {};

  private readonly providerSession: ProviderSession;

  public delInstanceTime(instance: string) {
    const time = this.configService.get<DelInstance>('DEL_INSTANCE');
    if (typeof time === 'number' && time > 0) {
      // Clear previous timeout if exists
      if (this.delInstanceTimeouts[instance]) {
        clearTimeout(this.delInstanceTimeouts[instance]);
      }

      // Set new timeout and store reference
      this.delInstanceTimeouts[instance] = setTimeout(
        async () => {
          try {
            if (this.waInstances[instance]?.connectionStatus?.state !== 'open') {
              if (this.waInstances[instance]?.connectionStatus?.state === 'connecting') {
                if ((await this.waInstances[instance].integration) === Integration.WHATSAPP_BAILEYS) {
                  await this.waInstances[instance]?.client?.logout('Log out instance: ' + instance);
                  this.waInstances[instance]?.client?.ws?.close();
                  this.waInstances[instance]?.client?.end(undefined);
                }
                this.eventEmitter.emit('remove.instance', instance, 'inner');
              } else {
                this.eventEmitter.emit('remove.instance', instance, 'inner');
              }
            }
          } finally {
            // Clean up timeout reference
            delete this.delInstanceTimeouts[instance];
          }
        },
        1000 * 60 * time,
      );
    }
  }

  public clearDelInstanceTime(instance: string) {
    if (this.delInstanceTimeouts[instance]) {
      clearTimeout(this.delInstanceTimeouts[instance]);
      delete this.delInstanceTimeouts[instance];
    }
  }

  public async instanceInfo(instanceNames?: string[], options: { withCounts?: boolean } = {}): Promise<any> {
    if (instanceNames && instanceNames.length > 0) {
      const inexistentInstances = instanceNames ? instanceNames.filter((instance) => !this.waInstances[instance]) : [];

      if (inexistentInstances.length > 0) {
        throw new NotFoundException(
          `Instance${inexistentInstances.length > 1 ? 's' : ''} "${inexistentInstances.join(', ')}" not found`,
        );
      }
    }

    const clientName = this.configService.get<Database>('DATABASE').CONNECTION.CLIENT_NAME;

    const where =
      instanceNames && instanceNames.length > 0
        ? {
            name: {
              in: instanceNames,
            },
            clientName,
          }
        : { clientName };

    const instances = await this.prismaRepository.instance.findMany({
      where,
      include: {
        Chatwoot: true,
        Proxy: true,
        Rabbitmq: true,
        Nats: true,
        Sqs: true,
        Websocket: true,
        Setting: true,
      },
    });

    if (!options.withCounts) {
      return instances;
    }

    const counts = await this.countRelationsByInstance(instances.map((instance) => instance.id));

    return instances.map((instance) => ({
      ...instance,
      _count: counts.get(instance.id),
    }));
  }

  // O include `_count` do Prisma gera `LEFT JOIN (SELECT "instanceId", COUNT(*) FROM "Contact" WHERE 1=1
  // GROUP BY "instanceId")`: conta as tabelas inteiras a cada chamada, mesmo quando só uma instância foi
  // pedida. Aqui cada contagem filtra pelos instanceIds pedidos e usa o índice de instanceId.
  private async countRelationsByInstance(instanceIds: string[]) {
    const counts = new Map<string, { Message: number; Contact: number; Chat: number }>(
      instanceIds.map((id) => [id, { Message: 0, Contact: 0, Chat: 0 }]),
    );

    if (instanceIds.length === 0) {
      return counts;
    }

    const where = { instanceId: { in: instanceIds } };
    const [messages, contacts, chats] = await Promise.all([
      this.prismaRepository.message.groupBy({ by: ['instanceId'], where, _count: { _all: true } }),
      this.prismaRepository.contact.groupBy({ by: ['instanceId'], where, _count: { _all: true } }),
      this.prismaRepository.chat.groupBy({ by: ['instanceId'], where, _count: { _all: true } }),
    ]);

    messages.forEach((row) => (counts.get(row.instanceId).Message = row._count._all));
    contacts.forEach((row) => (counts.get(row.instanceId).Contact = row._count._all));
    chats.forEach((row) => (counts.get(row.instanceId).Chat = row._count._all));

    return counts;
  }

  public async instanceInfoById(instanceId?: string, number?: string, options: { withCounts?: boolean } = {}) {
    let instanceName: string;
    if (instanceId) {
      instanceName = await this.prismaRepository.instance.findFirst({ where: { id: instanceId } }).then((r) => r?.name);
      if (!instanceName) {
        throw new NotFoundException(`Instance "${instanceId}" not found`);
      }
    } else if (number) {
      instanceName = await this.prismaRepository.instance.findFirst({ where: { number } }).then((r) => r?.name);
      if (!instanceName) {
        throw new NotFoundException(`Instance "${number}" not found`);
      }
    }

    if (!instanceName) {
      throw new NotFoundException(`Instance "${instanceId}" not found`);
    }

    if (instanceName && !this.waInstances[instanceName]) {
      throw new NotFoundException(`Instance "${instanceName}" not found`);
    }

    const instanceNames = instanceName ? [instanceName] : null;

    return this.instanceInfo(instanceNames, options);
  }

  public async cleaningUp(instanceName: string) {
    let instanceDbId: string;
    if (this.db.SAVE_DATA.INSTANCE) {
      const findInstance = await this.prismaRepository.instance.findFirst({
        where: { name: instanceName },
      });

      if (findInstance) {
        const instance = await this.prismaRepository.instance.update({
          where: { name: instanceName },
          data: { connectionStatus: 'close' },
        });

        rmSync(join(INSTANCE_DIR, instance.id), { recursive: true, force: true });

        instanceDbId = instance.id;
        await this.prismaRepository.session.deleteMany({ where: { sessionId: instance.id } });
      }
    }

    if (this.redis.REDIS.ENABLED && this.redis.REDIS.SAVE_INSTANCES) {
      await this.cache.delete(instanceName);
      if (instanceDbId) {
        await this.cache.delete(instanceDbId);
      }
    }

    if (this.providerSession?.ENABLED) {
      await this.providerFiles.removeSession(instanceName);
    }
  }

  public async cleaningStoreData(instanceName: string) {
    if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
      const instancePath = join(STORE_DIR, 'chatwoot', instanceName);
      execFileSync('rm', ['-rf', instancePath]);
    }

    const instance = await this.prismaRepository.instance.findFirst({
      where: { name: instanceName },
    });

    if (!instance) return;

    rmSync(join(INSTANCE_DIR, instance.id), { recursive: true, force: true });

    await this.prismaRepository.session.deleteMany({ where: { sessionId: instance.id } });

    await this.prismaRepository.chat.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.contact.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.messageUpdate.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.message.deleteMany({ where: { instanceId: instance.id } });

    await this.prismaRepository.webhook.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.chatwoot.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.proxy.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.rabbitmq.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.nats.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.sqs.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.integrationSession.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.typebot.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.websocket.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.setting.deleteMany({ where: { instanceId: instance.id } });
    await this.prismaRepository.label.deleteMany({ where: { instanceId: instance.id } });

    await this.prismaRepository.instance.delete({ where: { name: instanceName } });
  }

  public async loadInstance() {
    try {
      if (this.providerSession?.ENABLED) {
        await this.loadInstancesFromProvider();
      } else if (this.db.SAVE_DATA.INSTANCE) {
        await this.loadInstancesFromDatabasePostgres();
      } else if (this.redis.REDIS.ENABLED && this.redis.REDIS.SAVE_INSTANCES) {
        await this.loadInstancesFromRedis();
      }
    } catch (error) {
      this.logger.error(error);
    }
  }

  public async saveInstance(data: any) {
    try {
      const clientName = await this.configService.get<Database>('DATABASE').CONNECTION.CLIENT_NAME;
      await this.prismaRepository.instance.create({
        data: {
          id: data.instanceId,
          name: data.instanceName,
          ownerJid: data.ownerJid,
          profileName: data.profileName,
          profilePicUrl: data.profilePicUrl,
          connectionStatus:
            data.integration && data.integration === Integration.WHATSAPP_BAILEYS ? 'close' : (data.status ?? 'open'),
          number: data.number,
          integration: data.integration || Integration.WHATSAPP_BAILEYS,
          token: data.hash,
          clientName: clientName,
          businessId: data.businessId,
        },
      });
    } catch (error) {
      this.logger.error(error);
    }
  }

  public deleteInstance(instanceName: string) {
    try {
      this.eventEmitter.emit('remove.instance', instanceName, 'inner');
    } catch (error) {
      this.logger.error(error);
    }
  }

  public async forceDeleteInstance(instanceName: string) {
    try {
      await this.waInstances[instanceName]?.sendDataWebhook(Events.REMOVE_INSTANCE, null);
    } catch (error) {
      this.logger.error(error);
    }

    this.clearDelInstanceTime(instanceName);

    try {
      await this.cleaningUp(instanceName);
    } catch (error) {
      this.logger.error(error);
    }

    try {
      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
        const instancePath = join(STORE_DIR, 'chatwoot', instanceName);
        execFileSync('rm', ['-rf', instancePath]);
      }
    } catch (error) {
      this.logger.error(error);
    }

    const instance = await this.prismaRepository.instance.findFirst({
      where: { name: instanceName },
    });

    if (instance) {
      await this.prismaRepository.instance.delete({ where: { name: instanceName } });
    }

    try {
      delete this.waInstances[instanceName];
    } catch (error) {
      this.logger.error(error);
    }

    this.logger.warn(`Instance "${instanceName}" - FORCE REMOVED`);
  }

  // Registra a instância em memória sem conectar; a conexão do boot fica a cargo do connectOnBoot.
  private registerInstance(instanceData: BootInstance) {
    const instance = channelController.init(instanceData, {
      configService: this.configService,
      eventEmitter: this.eventEmitter,
      prismaRepository: this.prismaRepository,
      cache: this.cache,
      chatwootCache: this.chatwootCache,
      baileysCache: this.baileysCache,
      providerFiles: this.providerFiles,
    });

    if (!instance) return;

    instance.setInstance({
      instanceId: instanceData.instanceId,
      instanceName: instanceData.instanceName,
      integration: instanceData.integration,
      token: instanceData.token,
      number: instanceData.number,
      businessId: instanceData.businessId,
      ownerJid: instanceData.ownerJid,
    });

    this.waInstances[instanceData.instanceName] = instance;

    return instance;
  }

  // Reconecta no boot quem estava conectado (open/connecting) e também quem ficou 'close' por uma queda passageira:
  // a sessão continua válida, só faltou alguém reabrir. Sem isso, uma task morta no meio das reconexões deixa a
  // instância fora até alguém chamar /instance/connect (incidente de 30/10/2026: 394 instâncias, nem o rollback voltou).
  private shouldAutoConnect(instanceData: BootInstance) {
    if (instanceData.connectionStatus === 'open' || instanceData.connectionStatus === 'connecting') {
      return true;
    }

    return (
      instanceData.connectionStatus === 'close' &&
      WAMonitoringService.TRANSIENT_DISCONNECT_CODES.includes(instanceData.disconnectionReasonCode)
    );
  }

  private async setInstances(instancesData: BootInstance[]) {
    const toConnect: { instanceData: BootInstance; instance: any }[] = [];

    for (const instanceData of instancesData) {
      const instance = this.registerInstance(instanceData);
      if (!instance) continue;

      if (this.shouldAutoConnect(instanceData)) {
        toConnect.push({ instanceData, instance });
      } else {
        this.logger.info(
          `Skipping auto-connect for instance "${instanceData.instanceName}" (status: ${instanceData.connectionStatus || 'close'})`,
        );
      }
    }

    await this.connectOnBoot(toConnect);
  }

  // Conectar as ~500 instâncias de uma vez (handshake, sync inicial e fetch da versão do WhatsApp Web de todas ao
  // mesmo tempo) travava a CPU por minutos: as sockets caíam por timeout (408) e o health check matava a task.
  // Aqui no máximo BOOT_CONNECT_CONCURRENCY conectam juntas, e cada vaga só é liberada quando a instância abre ou
  // estoura o timeout.
  private async connectOnBoot(items: { instanceData: BootInstance; instance: any }[]) {
    if (items.length === 0) {
      return;
    }

    const parsed = Number.parseInt(process.env.BOOT_CONNECT_CONCURRENCY ?? '', 10);
    const concurrency = Number.isFinite(parsed) && parsed > 0 ? parsed : WAMonitoringService.BOOT_CONNECT_CONCURRENCY;
    const startedAt = Date.now();
    this.logger.info(`Boot: conectando ${items.length} instâncias, ${concurrency} por vez`);

    let next = 0;
    const worker = async () => {
      while (next < items.length) {
        const { instanceData, instance } = items[next++];
        try {
          this.logger.info(
            `Auto-connecting instance "${instanceData.instanceName}" (status: ${instanceData.connectionStatus})`,
          );
          await instance.connectToWhatsapp();
          if (instanceData.integration === Integration.WHATSAPP_BAILEYS) {
            await this.waitForOpen(instance, WAMonitoringService.BOOT_CONNECT_TIMEOUT_MS);
          }
        } catch (error) {
          this.logger.error(`Instance "${instanceData.instanceName}" - falha ao conectar no boot: ${error?.message}`);
        }
      }
    };

    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));

    this.logger.info(`Boot: ${items.length} instâncias processadas em ${Math.round((Date.now() - startedAt) / 1000)}s`);
  }

  private async waitForOpen(instance: any, timeoutMs: number) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (instance.connectionStatus?.state === 'open') {
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  private async loadInstancesFromRedis() {
    const keys = await this.cache.keys();

    if (!keys?.length) {
      return;
    }

    const instances = await Promise.all(
      keys.map(async (k): Promise<BootInstance | null> => {
        const instanceData = await this.prismaRepository.instance.findUnique({
          where: { id: k.split(':')[1] },
        });

        if (!instanceData) {
          return null;
        }

        return {
          instanceId: k.split(':')[1],
          instanceName: k.split(':')[2],
          integration: instanceData.integration,
          token: instanceData.token,
          number: instanceData.number,
          businessId: instanceData.businessId,
          connectionStatus: instanceData.connectionStatus as any, // Pass connection status
          disconnectionReasonCode: instanceData.disconnectionReasonCode,
        };
      }),
    );

    await this.setInstances(instances.filter(Boolean));
  }

  private async loadInstancesFromDatabasePostgres() {
    const clientName = await this.configService.get<Database>('DATABASE').CONNECTION.CLIENT_NAME;

    const instances = await this.prismaRepository.instance.findMany({
      where: { clientName: clientName },
    });

    if (instances.length === 0) {
      return;
    }

    await this.setInstances(
      instances.map((instance) => ({
        instanceId: instance.id,
        instanceName: instance.name,
        integration: instance.integration,
        token: instance.token,
        number: instance.number,
        businessId: instance.businessId,
        ownerJid: instance.ownerJid,
        connectionStatus: instance.connectionStatus as any, // Pass connection status
        disconnectionReasonCode: instance.disconnectionReasonCode,
      })),
    );
  }

  private async loadInstancesFromProvider() {
    const [instances] = await this.providerFiles.allInstances();

    if (!instances?.data) {
      return;
    }

    const rows = await Promise.all(
      instances.data.map((instanceId: string) =>
        this.prismaRepository.instance.findUnique({ where: { id: instanceId } }),
      ),
    );

    await this.setInstances(
      rows.filter(Boolean).map((instance) => ({
        instanceId: instance.id,
        instanceName: instance.name,
        integration: instance.integration,
        token: instance.token,
        businessId: instance.businessId,
        connectionStatus: instance.connectionStatus as any, // Pass connection status
        disconnectionReasonCode: instance.disconnectionReasonCode,
      })),
    );
  }

  private removeInstance() {
    this.eventEmitter.on('remove.instance', async (instanceName: string) => {
      try {
        await this.waInstances[instanceName]?.sendDataWebhook(Events.REMOVE_INSTANCE, null);

        this.clearDelInstanceTime(instanceName);

        this.cleaningUp(instanceName);
        this.cleaningStoreData(instanceName);
      } finally {
        this.logger.warn(`Instance "${instanceName}" - REMOVED`);
      }

      try {
        delete this.waInstances[instanceName];
      } catch (error) {
        this.logger.error(error);
      }
    });
    this.eventEmitter.on('logout.instance', async (instanceName: string) => {
      try {
        await this.waInstances[instanceName]?.sendDataWebhook(Events.LOGOUT_INSTANCE, null);

        this.clearDelInstanceTime(instanceName);

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
          this.waInstances[instanceName]?.clearCacheChatwoot();
        }

        this.cleaningUp(instanceName);
      } finally {
        this.logger.warn(`Instance "${instanceName}" - LOGOUT`);
      }
    });
  }

  private noConnection() {
    this.eventEmitter.on('no.connection', async (instanceName) => {
      try {
        await this.waInstances[instanceName]?.client?.logout('Log out instance: ' + instanceName);

        this.waInstances[instanceName]?.client?.ws?.close();

        this.waInstances[instanceName].instance.qrcode = { count: 0 };
        this.waInstances[instanceName].stateConnection.state = 'close';
      } catch (error) {
        this.logger.error({
          localError: 'noConnection',
          warn: 'Error deleting instance from memory.',
          error,
        });
      } finally {
        this.logger.warn(`Instance "${instanceName}" - NOT CONNECTION`);
        // Limite de QR atingido: a instância fica fechada em memória, e o banco precisa dizer o mesmo. Antes ele
        // ficava com o último status gravado ('connecting'/'open'), e quem lê o banco via a instância como viva.
        await this.prismaRepository.instance
          .updateMany({
            where: { name: instanceName },
            data: { connectionStatus: 'close', disconnectionAt: new Date() },
          })
          .catch((error) =>
            this.logger.error(`Instance "${instanceName}" - falha ao gravar close no banco: ${error?.message}`),
          );
      }
    });
  }
}
