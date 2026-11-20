/**
 * CDK entry point of the AWS side of the weekly managed-database matrix.
 *
 * One stack holds one run: a private-only VPC, one Secrets Manager credential per database, and one
 * database per target row. Every number here is the smallest setting that survives the suite — the
 * instance class is the floor that fits row-based binlog replication, the versions are the builds CI
 * pins, and nothing scales past the row.
 *
 * A row is one of three shapes, and which one it is costs real money, so the table says so:
 *
 * * a plain RDS instance (the bulk of the matrix — one instance, single AZ),
 * * a Multi-AZ instance (`multiAz: true`): the same instance plus the standby the failover scenario
 *   promotes, which is roughly twice the price and therefore only on the rows the scenario runs on,
 * * an Aurora cluster: a writer plus at least one reader, each a full instance of the row's class,
 *   because a cluster with nothing to promote is not a failover test.
 *
 * Nothing looks anything up in an account, so `cdk synth` runs offline and the composition can be
 * reviewed and asserted without cloud credentials. Run a subset by name:
 * `cdk synth -c targets=rds-mysql-84,aurora-pg-17 -c expiry=2026-11-18T14:00:00Z -c run-id=1234`.
 * The task that runs the suites exists only when the run hands the app a pinned runner image
 * (`-c runner-image=…` or `MATRIX_RUNNER_IMAGE`): a run that only provisions databases never
 * creates a cluster, a task definition or the endpoints a task needs, and a run that means to test
 * cannot do so with an unpinned build, because the image URI is checked where it is used.
 *
 * Cost of the whole table at `db.t4g.medium`, on-demand, one run: the nine plain instances are
 * ~$0.11 each, the three Multi-AZ rows ~$0.22 each, and the six clusters ~$0.21 each (two nodes), so
 * ~$3 per full run plus storage, backups and the capped logs. The weekly job runs a subset of named
 * rows; nothing here bills without a deploy. Nothing is ahead of this file any more: the parameter
 * groups with the binlog/GTID settings (PR A2), the Aurora and Multi-AZ failover variants (PR A3)
 * and the in-VPC task that runs the suites (PR A4) are all in it.
 */
import { App, Stack, type StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { MatrixAuroraCluster, MatrixClusterParameterGroup, clusterEngineFor } from '../lib/aurora-cluster';
import { MatrixArtifactBucket } from '../lib/bucket';
import { instanceEngineFor, MatrixDbInstance, MatrixEngine } from '../lib/db-instance';
import { MatrixMultiAzInstance } from '../lib/multiaz';
import { MatrixNetwork } from '../lib/network';
import { clusterClientEngineFor, MatrixClusterEngine, MatrixParameterGroup } from '../lib/params';
import { MatrixDatabaseCredential, MatrixRunner } from '../lib/runner';
import { MatrixAdminSecret } from '../lib/secret';
import { applyMatrixTags, expiryFromNow } from '../lib/tags';

/** What every row of the matrix names, whichever shape it is composed in. */
export interface MatrixTargetBase {
  readonly id: string;
  /** The build CI pins for this line, as RDS spells it. */
  readonly engineVersion: string;
  readonly instanceType: string;
}

/** A row composed as an RDS instance: one instance, optionally with the failover standby. */
export interface MatrixInstanceTarget extends MatrixTargetBase {
  readonly engine: MatrixEngine;
  /**
   * The standby in a second availability zone that the failover scenario promotes. It costs about as
   * much again as the instance, so it is asked for row by row and never by default.
   * @default false
   */
  readonly multiAz?: boolean;
}

/** A row composed as an Aurora cluster: a writer and the readers that make a failover possible. */
export interface MatrixClusterTarget extends MatrixTargetBase {
  readonly engine: MatrixClusterEngine;
  /**
   * Readers beside the writer, each in a zone of its own. @default the one a failover promotes
   * (see `lib/multiaz.ts`)
   */
  readonly readers?: number;
}

/** One row of the AWS matrix: an engine build and the smallest class that runs the suite on it. */
export type MatrixTarget = MatrixInstanceTarget | MatrixClusterTarget;

/** Whether a row is composed as a cluster rather than as an instance. */
export function isAuroraTarget(target: MatrixTarget): target is MatrixClusterTarget {
  return target.engine === MatrixClusterEngine.AuroraMySQL || target.engine === MatrixClusterEngine.AuroraPostgreSQL;
}

/**
 * The plain RDS rows of the matrix. MySQL 9.7 and 26.10 are absent on purpose: RDS offers the first
 * as a preview and the second not at all, so they stay in the local and CI matrix.
 */
export const RDS_TARGETS: readonly MatrixInstanceTarget[] = [
  { id: 'rds-mysql-80', engine: MatrixEngine.MySQL, engineVersion: '8.0.46', instanceType: 'db.t4g.medium' },
  { id: 'rds-mysql-84', engine: MatrixEngine.MySQL, engineVersion: '8.4.13', instanceType: 'db.t4g.medium' },
  { id: 'rds-mariadb-106', engine: MatrixEngine.MariaDB, engineVersion: '10.6.28', instanceType: 'db.t4g.medium' },
  { id: 'rds-mariadb-1011', engine: MatrixEngine.MariaDB, engineVersion: '10.11.19', instanceType: 'db.t4g.medium' },
  { id: 'rds-mariadb-114', engine: MatrixEngine.MariaDB, engineVersion: '11.4.13', instanceType: 'db.t4g.medium' },
  { id: 'rds-pg-15', engine: MatrixEngine.PostgreSQL, engineVersion: '15.20', instanceType: 'db.t4g.medium' },
  { id: 'rds-pg-16', engine: MatrixEngine.PostgreSQL, engineVersion: '16.16', instanceType: 'db.t4g.medium' },
  { id: 'rds-pg-17', engine: MatrixEngine.PostgreSQL, engineVersion: '17.12', instanceType: 'db.t4g.medium' },
  { id: 'rds-pg-18', engine: MatrixEngine.PostgreSQL, engineVersion: '18.7', instanceType: 'db.t4g.medium' },
];

/**
 * The Multi-AZ rows: one per engine family, on the build that gates a release. The failover scenario
 * is the same code whatever the patch build underneath it (`tests/failover_source.rs`, promoted
 * endpoint, "resume without rebuild"), so a standby per patch build would buy a second bill and no
 * extra proof. These rows are what `reboot-db-instance --force-failover` runs against.
 */
export const MULTI_AZ_TARGETS: readonly MatrixInstanceTarget[] = [
  { id: 'rds-mysql-84-maz', engine: MatrixEngine.MySQL, engineVersion: '8.4.13', instanceType: 'db.t4g.medium', multiAz: true },
  { id: 'rds-mariadb-114-maz', engine: MatrixEngine.MariaDB, engineVersion: '11.4.13', instanceType: 'db.t4g.medium', multiAz: true },
  { id: 'rds-pg-17-maz', engine: MatrixEngine.PostgreSQL, engineVersion: '17.12', instanceType: 'db.t4g.medium', multiAz: true },
];

/**
 * The Aurora rows. The builds are the ones RDS spells and CDK's catalog carries, so a row names a
 * build the service really offers rather than a major the service fills in. Each cluster runs one
 * reader: enough for `failover-db-cluster` to promote, and no more than the scenario needs.
 */
export const AURORA_TARGETS: readonly MatrixClusterTarget[] = [
  { id: 'aurora-mysql-3', engine: MatrixClusterEngine.AuroraMySQL, engineVersion: '8.0.mysql_aurora.3.12.0', instanceType: 'db.t4g.medium' },
  { id: 'aurora-mysql-84', engine: MatrixClusterEngine.AuroraMySQL, engineVersion: '8.4.mysql_aurora.8.4.7', instanceType: 'db.t4g.medium' },
  { id: 'aurora-pg-15', engine: MatrixClusterEngine.AuroraPostgreSQL, engineVersion: '15.17', instanceType: 'db.t4g.medium' },
  { id: 'aurora-pg-16', engine: MatrixClusterEngine.AuroraPostgreSQL, engineVersion: '16.13', instanceType: 'db.t4g.medium' },
  { id: 'aurora-pg-17', engine: MatrixClusterEngine.AuroraPostgreSQL, engineVersion: '17.9', instanceType: 'db.t4g.medium' },
  { id: 'aurora-pg-18', engine: MatrixClusterEngine.AuroraPostgreSQL, engineVersion: '18.6', instanceType: 'db.t4g.medium' },
];

/** Every AWS row the matrix knows. A run picks names out of this; nothing here deploys by itself. */
export const MATRIX_TARGETS: readonly MatrixTarget[] = [...RDS_TARGETS, ...MULTI_AZ_TARGETS, ...AURORA_TARGETS];

/** Name the run's stack carries, and the cleanup job matches. */
export const MATRIX_STACK_NAME = 'cloud-db-matrix';

export interface MatrixStackProps extends StackProps {
  readonly targets: readonly MatrixTarget[];
  /** ISO-8601 UTC instant after which the cleanup job may delete everything the run created. */
  readonly expiry: string;
  readonly runId: string;
  /** Pinned ECR URI of the runner image. Without it the run provisions no test compute at all. */
  readonly runnerImage?: string;
}

export class MatrixStack extends Stack {
  readonly targets: readonly MatrixTarget[];
  /** The databases of the run with their credentials, in target order; the runner reads these. */
  readonly credentials: readonly MatrixDatabaseCredential[];
  /** The task the suites run on, or absent when the run asked for databases only. */
  readonly runner?: MatrixRunner;

  constructor(scope: Construct, id: string, props: MatrixStackProps) {
    super(scope, id, props);
    if (props.targets.length === 0) {
      throw new Error('a run with no targets bills nothing and tests nothing');
    }
    this.targets = props.targets;

    const network = new MatrixNetwork(this, 'Network');
    // One parameter group per engine family, shared by every row of that family: the settings are the
    // family's, and a second copy of them per instance would buy nothing.
    const parameterGroups = new Map<string, MatrixParameterGroup>();
    const clusterParameterGroups = new Map<string, MatrixClusterParameterGroup>();
    const credentials: MatrixDatabaseCredential[] = [];
    for (const target of props.targets) {
      const secret = new MatrixAdminSecret(this, `Admin-${target.id}`);
      // Every row the run deploys is a row the suites have to reach, a cluster included, so the
      // credential is taken before the shapes split: a cluster the task cannot reach is a row that
      // bills without being tested.
      credentials.push({
        id: target.id,
        engine: isAuroraTarget(target) ? clusterClientEngineFor(target.engine) : target.engine,
        secret,
      });
      if (isAuroraTarget(target)) {
        const family = parameterFamilyOf(target);
        let group = clusterParameterGroups.get(family);
        if (group === undefined) {
          group = new MatrixClusterParameterGroup(this, `ClusterParams-${family.replace(/[^A-Za-z0-9]/g, '-')}`, {
            engine: target.engine,
            engineVersion: target.engineVersion,
          });
          clusterParameterGroups.set(family, group);
        }
        new MatrixAuroraCluster(this, `Cluster-${target.id}`, {
          vpc: network.vpc,
          subnetGroup: network.databaseSubnetGroup,
          securityGroup: network.databaseSecurityGroup,
          engine: target.engine,
          engineVersion: target.engineVersion,
          instanceType: target.instanceType,
          secret,
          clusterParameterGroup: group,
          ...(target.readers === undefined ? {} : { readers: target.readers }),
        });
        continue;
      }

      const family = parameterFamilyOf(target);
      let parameterGroup = parameterGroups.get(family);
      if (parameterGroup === undefined) {
        parameterGroup = new MatrixParameterGroup(this, `Params-${family.replace(/[^A-Za-z0-9]/g, '-')}`, {
          engine: target.engine,
          engineVersion: target.engineVersion,
        });
        parameterGroups.set(family, parameterGroup);
      }
      const instanceProps = {
        vpc: network.vpc,
        subnetGroup: network.databaseSubnetGroup,
        securityGroup: network.databaseSecurityGroup,
        engine: target.engine,
        engineVersion: target.engineVersion,
        instanceType: target.instanceType,
        secret,
        parameterGroup,
      };
      // The failover rows and the plain rows differ by one thing — the standby — and by who pays for
      // it, so the choice is made here rather than two constructs guessing at it.
      if (target.multiAz) {
        new MatrixMultiAzInstance(this, `Db-${target.id}`, instanceProps);
      } else {
        new MatrixDbInstance(this, `Db-${target.id}`, instanceProps);
      }
    }
    this.credentials = credentials;

    if (props.runnerImage !== undefined) {
      // One bucket and one task for the run, not one per target: the binaries are the same build
      // for every row, the artifacts are kept apart by prefix, and one task definition is one set
      // of grants to review instead of one per database.
      const bucket = new MatrixArtifactBucket(this, 'Artifacts');
      this.runner = new MatrixRunner(this, 'Runner', {
        network,
        bucket,
        databases: credentials,
        imageUri: props.runnerImage,
        runId: props.runId,
      });
    }
  }
}

/**
 * The parameter group family a target's settings belong to. Asked of the engine object rather than
 * spelled out here, so the group and the database cannot end up configured for different engines.
 */
function parameterFamilyOf(target: MatrixTarget): string {
  const engine = isAuroraTarget(target)
    ? clusterEngineFor(target.engine, target.engineVersion)
    : instanceEngineFor(target.engine, target.engineVersion);
  const family = engine.parameterGroupFamily;
  if (family === undefined) {
    throw new Error(`target "${target.id}" runs a build with no parameter group family`);
  }
  return family;
}

export interface MatrixAppProps {
  /** Target ids to deploy. @default every row in MATRIX_TARGETS */
  readonly targets?: readonly string[];
  readonly expiry: string;
  readonly runId: string;
  /** Pinned ECR URI of the runner image; without it the run deploys databases and no compute. */
  readonly runnerImage?: string;
  /** @default MATRIX_STACK_NAME */
  readonly stackName?: string;
  /** Account and region the run deploys into. They come from the environment the workflow sets. */
  readonly env?: { readonly account?: string; readonly region?: string };
}

/** Builds one run's stack and tags everything it creates, on an app the caller owns. */
export function createMatrixStack(app: App, props: MatrixAppProps): MatrixStack {
  const stack = new MatrixStack(app, props.stackName ?? MATRIX_STACK_NAME, {
    env: props.env,
    targets: selectTargets(props.targets),
    expiry: props.expiry,
    runId: props.runId,
    runnerImage: props.runnerImage,
  });
  applyMatrixTags(stack, { expiry: props.expiry, runId: props.runId });
  return stack;
}

function selectTargets(ids?: readonly string[]): MatrixTarget[] {
  if (ids === undefined) {
    return MATRIX_TARGETS.map((target) => ({ ...target }));
  }
  if (ids.length === 0) {
    throw new Error('a run with no targets bills nothing and tests nothing');
  }
  return ids.map((id) => {
    const target = MATRIX_TARGETS.find((candidate) => candidate.id === id);
    if (!target) {
      throw new Error(`unknown target "${id}"; the matrix has ${MATRIX_TARGETS.map((candidate) => candidate.id).join(', ')}`);
    }
    return { ...target };
  });
}

/** Reads a comma-separated context value such as `-c targets=rds-mysql-84,aurora-pg-17`. */
function contextList(app: App, key: string): string[] | undefined {
  const raw = app.node.tryGetContext(key);
  if (typeof raw !== 'string' || raw.trim() === '') {
    return undefined;
  }
  return raw
    .split(',')
    .map((part) => part.trim())
    .filter((part) => part !== '');
}

function contextString(app: App, key: string): string | undefined {
  const raw = app.node.tryGetContext(key);
  return typeof raw === 'string' && raw.trim() !== '' ? raw : undefined;
}

function main(): void {
  const app = new App();
  const runId = contextString(app, 'run-id') ?? process.env.GITHUB_RUN_ID ?? 'local-run';
  // An env-agnostic stack still synthesizes (that is what lets the composition be reviewed with no
  // account), but a deploy needs both halves of a location, so they are taken as a pair.
  const account = process.env.CDK_DEFAULT_ACCOUNT;
  const region = process.env.CDK_DEFAULT_REGION;
  createMatrixStack(app, {
    targets: contextList(app, 'targets'),
    expiry: contextString(app, 'expiry') ?? expiryFromNow(),
    runId,
    runnerImage: contextString(app, 'runner-image') ?? process.env.MATRIX_RUNNER_IMAGE,
    env: account && region ? { account, region } : undefined,
  });
}

if (require.main === module) {
  main();
}
