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
  // O driver `pg` não interpreta sslmode=require da connection string como o
  // libpq (cifra sem verificar a cadeia) — sem ssl explícito ele faz
  // verificação estrita e rejeita o certificado da RDS como "self-signed".
  // Replicamos o mesmo nível de confiança do libpq (encrypt, don't verify).
  const requiresTls = /sslmode=(require|prefer|verify-ca|verify-full)/.test(connectionString);
  return new PrismaPg(requiresTls ? { connectionString, ssl: { rejectUnauthorized: false } } : connectionString);
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
