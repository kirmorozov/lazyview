/**
 * One managed database of the weekly test matrix.
 *
 * This is the shape every target row has on AWS: a single, private, encrypted RDS instance of the
 * smallest class that survives the suite, with automated backups on. Backups are not a
 * nice-to-have here: RDS keeps no binlogs without them, so an instance without backups would let
 * the binlog suite pass while testing nothing. Nothing scales — one instance, no storage growth
 * past what the run asks for — so a run that would cost more has nowhere to go.
 *
 * The engine's own settings — binlog format and image, GTID, logical replication — are a parameter
 * group, and the instance refuses to build without one: an instance that runs on the service's
 * defaults is not the server the suite thinks it is testing. Clusters and Multi-AZ failover
 * variants belong to PR A3.
 */
import { Annotations, Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import { Construct } from 'constructs';
import type { MatrixParameterGroup } from './params';
import { MatrixAdminSecret } from './secret';

/** The engine families the matrix runs on RDS, named the way RDS names them. */
export enum MatrixEngine {
  MySQL = 'mysql',
  MariaDB = 'mariadb',
  PostgreSQL = 'postgres',
}

/** Smallest storage RDS offers; a suite run never needs more and cost follows size. */
export const DEFAULT_ALLOCATED_STORAGE_GB = 20;

/** The shortest retention that keeps binlogs: RDS drops them when automated backups are off. */
export const MIN_BACKUP_RETENTION_DAYS = 1;

/** Logs a run collects to prove what the engine did. Capped: general query logs are not exported. */
const EXPORTED_LOG_TYPES: Record<MatrixEngine, readonly string[]> = {
  [MatrixEngine.MySQL]: ['error', 'slowquery'],
  [MatrixEngine.MariaDB]: ['error', 'slowquery'],
  [MatrixEngine.PostgreSQL]: ['postgresql'],
};

/** The engine logs a run exports and owns, for the engine family. */
export function exportedLogTypes(engine: MatrixEngine): readonly string[] {
  return EXPORTED_LOG_TYPES[engine];
}

const INSTANCE_TYPE_PATTERN = /^db\.[a-z0-9]+\.[a-z0-9]+$/;

export interface MatrixDbInstanceProps {
  readonly vpc: ec2.IVpc;
  /** The run's private subnet group (two AZs, DESTROYed with the run). */
  readonly subnetGroup: rds.SubnetGroup;
  /** The run's database group: private, no egress, ingress only from the test task. */
  readonly securityGroup: ec2.ISecurityGroup;
  readonly engine: MatrixEngine;
  /** Full dotted build, as RDS spells it (the same build CI pins), never a floating major. */
  readonly engineVersion: string;
  /** Full instance type, e.g. `db.t4g.medium`. */
  readonly instanceType: string;
  readonly secret: MatrixAdminSecret;
  /**
   * The engine settings of the target's family, created once per family by the run and shared by
   * every instance of it. Required: an instance on the service's defaults is a different server
   * from the one the suite measures.
   */
  readonly parameterGroup: MatrixParameterGroup;
  /** Seed schema the bootstrap SQL creates the suite's accounts in. @default DEFAULT_DATABASE_NAME */
  readonly databaseName?: string;
  /** @default DEFAULT_ALLOCATED_STORAGE_GB */
  readonly allocatedStorage?: number;
  /** @default MIN_BACKUP_RETENTION_DAYS */
  readonly backupRetentionDays?: number;
  /** Only the failover rows ask for a standby; every other run stays single-AZ. @default false */
  readonly multiAz?: boolean;
  /** Key the storage is encrypted with. @default a new key the run owns and destroys */
  readonly storageEncryptionKey?: kms.IKey;
}

/** Seed database of a run: the same name `docker/initdb` creates locally, so the suites match. */
export const DEFAULT_DATABASE_NAME = 'lvtest';

export class MatrixDbInstance extends Construct {
  readonly instance: rds.DatabaseInstance;
  readonly storageEncryptionKey: kms.IKey;

  constructor(scope: Construct, id: string, props: MatrixDbInstanceProps) {
    super(scope, id);
    const allocatedStorage = props.allocatedStorage ?? DEFAULT_ALLOCATED_STORAGE_GB;
    if (!Number.isInteger(allocatedStorage) || allocatedStorage < DEFAULT_ALLOCATED_STORAGE_GB) {
      throw new Error(`allocated storage must be a whole number of GiB at or above ${DEFAULT_ALLOCATED_STORAGE_GB}, got ${allocatedStorage}`);
    }
    const backupRetentionDays = props.backupRetentionDays ?? MIN_BACKUP_RETENTION_DAYS;
    if (backupRetentionDays < MIN_BACKUP_RETENTION_DAYS) {
      throw new Error(
        `backup retention must be at least ${MIN_BACKUP_RETENTION_DAYS} day: without automated backups RDS keeps no binlogs and the binlog suite tests nothing`,
      );
    }
    if (!INSTANCE_TYPE_PATTERN.test(props.instanceType)) {
      throw new Error(`instance type "${props.instanceType}" is not a full RDS class such as db.t4g.medium`);
    }
    // The group is shared across a family, so the check that matters is that it is the family of
    // *this* engine: binlog settings in a group attached to the wrong kind of database configure
    // nothing, and the run would go on measuring an unconfigured server.
    assertParameterGroupFamilyFor(props.engine, props.parameterGroup.family);

    this.storageEncryptionKey =
      props.storageEncryptionKey ??
      new kms.Key(this, 'StorageKey', {
        description: 'Storage of the managed databases of one test-matrix run',
        enableKeyRotation: true,
        removalPolicy: RemovalPolicy.DESTROY,
      });
    // The engine, not the run, reads the storage, so the engine is the principal the key serves.
    this.storageEncryptionKey.grantEncryptDecrypt(new iam.ServicePrincipal('rds.amazonaws.com'));

    this.instance = new rds.DatabaseInstance(this, 'Db', {
      engine: instanceEngineFor(props.engine, props.engineVersion),
      // The family's engine settings, never the service's defaults.
      parameterGroup: props.parameterGroup.group,
      credentials: rds.Credentials.fromSecret(props.secret.secret, props.secret.username),
      vpc: props.vpc,
      // The run's private subnet group, which the network owns and destroys: a database in here
      // never gets a public endpoint, and the suites run inside the VPC.
      subnetGroup: props.subnetGroup,
      securityGroups: [props.securityGroup],
      publiclyAccessible: false,
      // CDK puts the `db.` prefix on itself; the catalog spells the full class for the reader.
      instanceType: new ec2.InstanceType(props.instanceType.replace(/^db\./, '')),
      // No cluster: a failover row asks for Multi-AZ explicitly, and nothing else pays for one.
      multiAz: props.multiAz ?? false,
      allocatedStorage,
      storageType: rds.StorageType.GP3,
      storageEncrypted: true,
      storageEncryptionKey: this.storageEncryptionKey,
      databaseName: props.databaseName ?? DEFAULT_DATABASE_NAME,
      backupRetention: Duration.days(backupRetentionDays),
      deleteAutomatedBackups: true,
      // Teardown is by tag and must never be blocked; DESTROY is what keeps a run from leaking.
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
      // A run tests the build its row names, not one the service picked under it.
      autoMinorVersionUpgrade: false,
      cloudwatchLogsExports: [...EXPORTED_LOG_TYPES[props.engine]],
    });

    // The engine writes these logs whether or not we ask, so the run owns the groups: named the
    // way RDS expects, capped to a week, and DESTROYed with the rest of the run instead of
    // accumulating in the account long after the databases are gone.
    for (const logType of EXPORTED_LOG_TYPES[props.engine]) {
      new logs.LogGroup(this, `Log-${logType}`, {
        logGroupName: `/aws/rds/instance/${this.instance.instanceIdentifier}/${logType}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      });
    }

    // The instance builds are the ones CI pins; the CloudFormation spec bundled with this CDK
    // release can predate them, which makes its version list stale rather than a fact about RDS.
    Annotations.of(this).acknowledgeWarning(
      CFN_ENGINE_VERSION_RULE_ID,
      `the engine build is the one the matrix row pins (${props.engineVersion}); the CloudFormation spec bundled with this CDK release can lag behind what RDS offers`,
    );
  }
}

/** The synth rule that compares a pinned build against the bundled spec's list. */
export const CFN_ENGINE_VERSION_RULE_ID = 'CloudFormation-Validate::E9006';

/**
 * The engine RDS is asked for, from the full build string. The major is what RDS matches a
 * parameter group family on, so it is taken from the same string rather than a second source — the
 * instance and its parameter group are both built from it and cannot drift apart.
 */
export function instanceEngineFor(engine: MatrixEngine, version: string): rds.IInstanceEngine {
  const parts = version.split('.');
  if (parts.length < 2 || parts.some((part) => !/^\d+$/.test(part))) {
    throw new Error(`engine version "${version}" is not a full dotted build such as 8.4.13`);
  }
  if (engine === MatrixEngine.MySQL) {
    return rds.DatabaseInstanceEngine.mysql({ version: rds.MysqlEngineVersion.of(version, majorOf(version, 2)) });
  }
  if (engine === MatrixEngine.MariaDB) {
    return rds.DatabaseInstanceEngine.mariaDb({ version: rds.MariaDbEngineVersion.of(version, majorOf(version, 2)) });
  }
  return rds.DatabaseInstanceEngine.postgres({ version: rds.PostgresEngineVersion.of(version, majorOf(version, 1)) });
}

/**
 * Checks a parameter group family against the engine it is about to configure, and returns it.
 * RDS names a family after the engine it configures (`mysql8.4`, `mariadb10.11`, `postgres17`), so
 * a family that does not start with the engine is a group attached to the wrong kind of database:
 * it would set binlog parameters nothing reads, and the run would measure an unconfigured server.
 */
export function assertParameterGroupFamilyFor(engine: MatrixEngine, family: string): string {
  if (!family.startsWith(engine)) {
    throw new Error(`parameter group family "${family}" configures something other than a "${engine}" database`);
  }
  return family;
}

function majorOf(version: string, components: number): string {
  return version.split('.').slice(0, components).join('.');
}
