import { ConfigService, Database } from '@config/env.config';
import { Logger } from '@config/logger.config';
import { PrismaMariaDb } from '@prisma/adapter-mariadb';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';

export class Query<T> {
  where?: T;
  sort?: 'asc' | 'desc';
  page?: number;
  offset?: number;
}

// Prisma 7 exige driver adapter. Seleciona o adapter conforme DATABASE_PROVIDER.
function createPrismaAdapter(connectionString: string) {
  const provider = process.env.DATABASE_PROVIDER ?? 'postgresql';
  if (provider === 'mysql') {
    return new PrismaMariaDb(connectionString);
  }
  // postgresql e psql_bouncer usam o adapter do Postgres.
  // pg-connection-string (usado pelo @prisma/adapter-pg) trata sslmode=require
  // como alias de verify-full, não como o libpq clássico (cifra sem verificar
  // a cadeia) — rejeita o certificado da RDS como "self-signed certificate in
  // certificate chain". Um objeto `ssl` explícito por cima NÃO sobrepõe esse
  // parsing (testado). uselibpqcompat=true restaura a semântica clássica do
  // libpq pro sslmode=require, como o próprio pg recomenda no aviso de
  // deprecação. Ver: https://github.com/brianc/node-postgres/pull/3392
  const requiresTls = /sslmode=(require|prefer|verify-ca|verify-full)/.test(connectionString);
  const finalConnectionString = requiresTls
    ? `${connectionString}${connectionString.includes('?') ? '&' : '?'}uselibpqcompat=true`
    : connectionString;
  return new PrismaPg(finalConnectionString);
}

export class PrismaRepository extends PrismaClient {
  constructor(private readonly configService: ConfigService) {
    super({ adapter: createPrismaAdapter(configService.get<Database>('DATABASE').CONNECTION.URI) });
  }

  private readonly logger = new Logger('PrismaRepository');

  public async onModuleInit() {
    await this.$connect();
    this.logger.info('Repository:Prisma - ON');
  }

  public async onModuleDestroy() {
    await this.$disconnect();
    this.logger.warn('Repository:Prisma - OFF');
  }
}
