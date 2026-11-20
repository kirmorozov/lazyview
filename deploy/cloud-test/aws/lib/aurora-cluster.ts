/**
 * One Aurora cluster of the weekly test matrix.
 *
 * An Aurora row is the managed-service answer to a question the single-instance rows cannot ask:
 * what does lazyview do when the node it was reading from stops and a *different* node takes the
 * endpoint? The scenario itself is the GTID plan's (`tests/failover_source.rs`, run against the
 * promoted endpoint), so this file composes the shape that scenario needs and nothing else — a
 * writer, at least one reader in another availability zone, and the cluster parameter group whose
 * settings make the binlog exist in the first place.
 *
 * Two things about Aurora change what "configured" means, and both are why the group is a cluster
 * parameter group rather than the RDS one:
 *
 * * The binlog is off until a *cluster* parameter turns it on. Aurora's reference says `log-bin` is
 *   not settable on its own and follows `binlog_format`, whose family default is `OFF`. A cluster
 *   that inherits the default group has no binlog, so a run against it would pass without ever
 *   reading a change. That is why the group is required here, exactly as it is on an instance.
 * * The commit-order proof comes from the enhanced binlog. `src/fence.rs:37-44` accepts an
 *   `aurora_enhanced_binlog` status of `ACTIVE` in place of `binlog_order_commits`, and the
 *   enhanced binlog is also the only way to read the log while it is written. It needs
 *   `binlog_backup` and `binlog_replication_globaldb` off, which is what AWS's own prerequisites for
 *   reading an Aurora binlog say. GTIDs are not asked for: on Aurora the parameter is only
 *   *sometimes* modifiable (a staged transition plus a reboot), and `src/engine.rs:117-119` withholds
 *   the GTID advice on Aurora too, "where the parameter does not exist as such".
 *
 * Cost: a cluster bills one instance of the row's class per node, so a row costs its instance class
 * times writer plus readers — about twice a single-AZ RDS row at `db.t4g.medium`. There is no
 * autoscaling and no serverless capacity here, so a run has nothing to scale into and nowhere to
 * over-run; the whole cluster is DESTROYed with the run. Nothing in this file runs per binlog event.
 */
import { Annotations, Duration, RemovalPolicy, Stack } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as rds from 'aws-cdk-lib/aws-rds';
import { Construct } from 'constructs';
import { CFN_ENGINE_VERSION_RULE_ID, assertInstanceClass, MIN_BACKUP_RETENTION_DAYS } from './db-instance';
import {
  auroraClusterParametersFor,
  CLUSTER_FORBIDDEN_PARAMETERS,
  CLUSTER_REQUIRED_PARAMETERS,
  MatrixClusterEngine,
} from './params';
import type { MatrixAdminSecret } from './secret';
import { failoverZones, requireStandby } from './multiaz';

/** The build RDS spells for Aurora MySQL: the MySQL line, then the Aurora build. */
const AURORA_MYSQL_BUILD = /^(\d+\.\d+)\.mysql_aurora\.(\d+\.\d+\.\d+)$/;
/** The build RDS spells for Aurora PostgreSQL: the major and the patch release. */
const AURORA_POSTGRES_BUILD = /^(\d+)\.(\d+)$/;

/** The engine logs a run collects from each Aurora family. Capped: no general query log. */
const EXPORTED_CLUSTER_LOG_TYPES: Readonly<Record<MatrixClusterEngine, readonly string[]>> = {
  [MatrixClusterEngine.AuroraMySQL]: ['error', 'slowquery'],
  [MatrixClusterEngine.AuroraPostgreSQL]: ['postgresql'],
};

/** The logs a run exports and owns, for the Aurora family. */
export function exportedClusterLogTypes(engine: MatrixClusterEngine): readonly string[] {
  return EXPORTED_CLUSTER_LOG_TYPES[engine];
}

/**
 * The Aurora engine RDS is asked for, from the build the row pins. The major — the part RDS names a
 * parameter group family after — is read out of the same string rather than passed separately, so a
 * cluster and the group that configures it cannot disagree about which build they are.
 */
export function clusterEngineFor(engine: MatrixClusterEngine, version: string): rds.IClusterEngine {
  if (engine === MatrixClusterEngine.AuroraMySQL) {
    const build = AURORA_MYSQL_BUILD.exec(version);
    if (!build) {
      throw new Error(`Aurora MySQL build "${version}" is not the build RDS spells, such as 8.0.mysql_aurora.3.12.0`);
    }
    return rds.DatabaseClusterEngine.auroraMysql({ version: rds.AuroraMysqlEngineVersion.of(version, build[1]!) });
  }
  const build = AURORA_POSTGRES_BUILD.exec(version);
  if (!build) {
    throw new Error(`Aurora PostgreSQL build "${version}" is not the build RDS spells, such as 16.13`);
  }
  return rds.DatabaseClusterEngine.auroraPostgres({ version: rds.AuroraPostgresEngineVersion.of(version, build[1]!) });
}

/**
 * Checks a cluster parameter group family against the Aurora engine it is about to configure, and
 * returns it. RDS names the family after the engine and the major (`aurora-mysql8.0`,
 * `aurora-postgresql16`), so a family that does not start with the engine names something else: a
 * group like that sets binlog parameters nothing reads, and the run measures an unconfigured cluster.
 */
export function assertClusterParameterGroupFamilyFor(engine: MatrixClusterEngine, family: string): string {
  if (!family.startsWith(engine)) {
    throw new Error(`cluster parameter group family "${family}" configures something other than a "${engine}" cluster`);
  }
  return family;
}

export interface MatrixClusterParameterGroupProps {
  readonly engine: MatrixClusterEngine;
  /** The same full build the cluster runs, so the family is derived once, not twice. */
  readonly engineVersion: string;
  /** Extra settings for one row, merged over the family's set. @default none */
  readonly extraParameters?: Readonly<Record<string, string>>;
}

/**
 * The cluster parameter group of one Aurora family: every cluster of that family in the run shares
 * it, because the settings belong to the family and a second copy per cluster buys nothing.
 */
export class MatrixClusterParameterGroup extends Construct {
  /** The family the group configures, as RDS names it (`aurora-mysql8.0`, `aurora-postgresql16`). */
  readonly family: string;
  /** The settings this group carries, for the run's own checks and for the assertions. */
  readonly parameters: Readonly<Record<string, string>>;
  readonly group: rds.IParameterGroup;

  constructor(scope: Construct, id: string, props: MatrixClusterParameterGroupProps) {
    super(scope, id);
    const engine = clusterEngineFor(props.engine, props.engineVersion);
    const family = engine.parameterGroupFamily;
    if (!family) {
      throw new Error(`Aurora build "${props.engineVersion}" has no parameter group family`);
    }
    assertClusterParameterGroupFamilyFor(props.engine, family);

    this.parameters = { ...auroraClusterParametersFor(props.engine), ...(props.extraParameters ?? {}) };
    const missing = CLUSTER_REQUIRED_PARAMETERS[props.engine].filter((name) => this.parameters[name] === undefined);
    if (missing.length > 0) {
      throw new Error(`the ${family} cluster group would leave ${missing.join(', ')} to the engine default`);
    }
    const forbidden = CLUSTER_FORBIDDEN_PARAMETERS[props.engine].filter((name) => this.parameters[name] !== undefined);
    if (forbidden.length > 0) {
      throw new Error(`the ${family} family does not offer ${forbidden.join(', ')}: RDS would refuse the group, or the setting would replace the one the row is there to measure`);
    }

    this.family = family;
    this.group = new rds.ParameterGroup(this, 'Group', {
      engine,
      description: `Engine settings of the ${family} clusters of one managed-database test-matrix run`,
      parameters: { ...this.parameters },
      // Teardown is by tag, and a group left behind is a group the next run cannot create again.
      removalPolicy: RemovalPolicy.DESTROY,
    });
  }
}

export interface MatrixAuroraClusterProps {
  readonly vpc: ec2.IVpc;
  /** The run's private subnet group (two AZs, DESTROYed with the run). */
  readonly subnetGroup: rds.SubnetGroup;
  /** The run's database group: private, no egress, ingress only from the test task. */
  readonly securityGroup: ec2.ISecurityGroup;
  readonly engine: MatrixClusterEngine;
  /** Full build as RDS spells it (`8.0.mysql_aurora.3.12.0`, `16.13`), never a floating major. */
  readonly engineVersion: string;
  /** Full instance type, e.g. `db.t4g.medium`; every node of the cluster is this class. */
  readonly instanceType: string;
  readonly secret: MatrixAdminSecret;
  /**
   * The cluster settings of the row's family, created once per family and shared by every cluster of
   * it. Required for the same reason an instance requires its group: a cluster on the default group
   * has its binlog switched off, so the run would read nothing and notice nothing.
   */
  readonly clusterParameterGroup: MatrixClusterParameterGroup;
  /** Readers beside the writer. @default MIN_STANDBY_INSTANCES, the one the failover promotes */
  readonly readers?: number;
  /** Zones to spread the nodes over. @default the stack's zones */
  readonly availabilityZones?: readonly string[];
  /** @default MIN_BACKUP_RETENTION_DAYS */
  readonly backupRetentionDays?: number;
  /** Key the cluster storage is encrypted with. @default a new key the run owns and destroys */
  readonly storageEncryptionKey?: kms.IKey;
}

/**
 * One Aurora row of the matrix: a writer and at least one reader, private and encrypted, with the
 * cluster group that turns its binlog on. Every node is a full instance of the row's class, in a
 * zone of its own, because the row exists to be failed over.
 */
export class MatrixAuroraCluster extends Construct {
  readonly cluster: rds.DatabaseCluster;
  readonly readerCount: number;
  readonly storageEncryptionKey: kms.IKey;

  constructor(scope: Construct, id: string, props: MatrixAuroraClusterProps) {
    super(scope, id);
    // The rule the row exists for: without a reader there is nothing to promote, and the failover
    // scenario would rehearse a reboot and call it a promotion.
    this.readerCount = requireStandby('reader in another availability zone', props.readers ?? 1);
    if (props.backupRetentionDays !== undefined && props.backupRetentionDays < MIN_BACKUP_RETENTION_DAYS) {
      throw new Error(
        `cluster backup retention must be at least ${MIN_BACKUP_RETENTION_DAYS} day: without automated backups Aurora keeps no binlogs and the binlog suite tests nothing`,
      );
    }
    assertInstanceClass(props.instanceType);
    // The group is shared across a family, so what matters here is that it is the family of *this*
    // engine: binlog settings in a group attached to another kind of cluster configure nothing.
    assertClusterParameterGroupFamilyFor(props.engine, props.clusterParameterGroup.family);

    // The writer first, then one reader per zone after it: the nodes of a failover row are never in
    // the same zone, which is what gives the scenario something to promote that the outage also took.
    const nodes = this.readerCount + 1;
    const zones = failoverZones(props.availabilityZones ?? [...Stack.of(this).availabilityZones], nodes, 'cluster node');
    const instanceType = new ec2.InstanceType(props.instanceType.replace(/^db\./, ''));
    const writer = rds.ClusterInstance.provisioned('Writer', {
      instanceType,
      availabilityZone: zones[0],
      publiclyAccessible: false,
    });
    const readers = Array.from({ length: this.readerCount }, (_unused, index) =>
      rds.ClusterInstance.provisioned(`Reader-${index + 1}`, {
        instanceType,
        availabilityZone: zones[(index + 1) % zones.length]!,
        publiclyAccessible: false,
      }),
    );

    this.storageEncryptionKey =
      props.storageEncryptionKey ??
      new kms.Key(this, 'StorageKey', {
        description: 'Storage of the Aurora clusters of one test-matrix run',
        enableKeyRotation: true,
        removalPolicy: RemovalPolicy.DESTROY,
      });
    // The engine reads the storage, so the engine is the principal the key serves.
    this.storageEncryptionKey.grantEncryptDecrypt(new iam.ServicePrincipal('rds.amazonaws.com'));

    this.cluster = new rds.DatabaseCluster(this, 'Cluster', {
      engine: clusterEngineFor(props.engine, props.engineVersion),
      // The family's cluster settings, never the service's defaults.
      parameterGroup: props.clusterParameterGroup.group,
      credentials: rds.Credentials.fromSecret(props.secret.secret, props.secret.username),
      vpc: props.vpc,
      // The run's own private subnet group: a node in here never gets a public endpoint, and the
      // suites reach the cluster from inside the VPC.
      subnetGroup: props.subnetGroup,
      securityGroups: [props.securityGroup],
      writer,
      readers,
      storageEncrypted: true,
      storageEncryptionKey: this.storageEncryptionKey,
      backup: { retention: Duration.days(props.backupRetentionDays ?? MIN_BACKUP_RETENTION_DAYS) },
      // Teardown is by tag and must never be blocked; DESTROY is what keeps a run from leaking.
      deletionProtection: false,
      removalPolicy: RemovalPolicy.DESTROY,
      // A run tests the build its row names, not one the service picked under it.
      autoMinorVersionUpgrade: false,
      cloudwatchLogsExports: [...EXPORTED_CLUSTER_LOG_TYPES[props.engine]],
    });

    // CDK imports these log groups rather than creating them, so the run creates the ones it names:
    // capped to a week and DESTROYed with the rest of the run, instead of being left to the service
    // to make on demand and keep after the cluster is gone.
    for (const logType of EXPORTED_CLUSTER_LOG_TYPES[props.engine]) {
      new logs.LogGroup(this, `Log-${logType}`, {
        logGroupName: `/aws/rds/cluster/${this.cluster.clusterIdentifier}/${logType}`,
        retention: logs.RetentionDays.ONE_WEEK,
        removalPolicy: RemovalPolicy.DESTROY,
      });
    }

    // The builds are the ones the matrix rows pin; the CloudFormation spec bundled with this CDK
    // release can predate them, which makes its version list stale rather than a fact about Aurora.
    Annotations.of(this).acknowledgeWarning(
      CFN_ENGINE_VERSION_RULE_ID,
      `the engine build is the one the matrix row pins (${props.engineVersion}); the CloudFormation spec bundled with this CDK release can lag behind what Aurora offers`,
    );
  }
}
