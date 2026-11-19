/**
 * CDK entry point of the AWS side of the weekly managed-database matrix.
 *
 * One stack holds one run: a private-only VPC, one Secrets Manager credential per database, and
 * one RDS instance per target row. Every number here is the smallest setting that survives the
 * suite — the instance class is the floor that fits row-based binlog replication, the versions are
 * the builds CI pins, and nothing scales past a single instance.
 *
 * Nothing looks anything up in an account, so `cdk synth` runs offline and the composition can be
 * reviewed and asserted without cloud credentials. Run a subset by name:
 * `cdk synth -c targets=rds-mysql-84,rds-pg-17 -c expiry=2026-11-18T14:00:00Z -c run-id=1234`.
 *
 * Still ahead of this file: parameter groups with the binlog/GTID settings (PR A2), Aurora and
 * Multi-AZ failover variants (PR A3), and the in-VPC task that runs the suites (PR A4).
 */
import { App, Stack, type StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { instanceEngineFor, MatrixDbInstance, MatrixEngine } from '../lib/db-instance';
import { MatrixNetwork } from '../lib/network';
import { MatrixParameterGroup } from '../lib/params';
import { MatrixAdminSecret } from '../lib/secret';
import { applyMatrixTags, expiryFromNow } from '../lib/tags';

/** One row of the AWS matrix: an engine build and the smallest class that runs the suite on it. */
export interface MatrixTarget {
  readonly id: string;
  readonly engine: MatrixEngine;
  /** The build CI pins for this line, as RDS spells it. */
  readonly engineVersion: string;
  readonly instanceType: string;
}

/**
 * The RDS rows of the matrix. MySQL 9.7 and 26.10 are absent on purpose: RDS offers the first as a
 * preview and the second not at all, so they stay in the local and CI matrix.
 */
export const RDS_TARGETS: readonly MatrixTarget[] = [
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

/** Name the run's stack carries, and the cleanup job matches. */
export const MATRIX_STACK_NAME = 'cloud-db-matrix';

export interface MatrixStackProps extends StackProps {
  readonly targets: readonly MatrixTarget[];
  /** ISO-8601 UTC instant after which the cleanup job may delete everything the run created. */
  readonly expiry: string;
  readonly runId: string;
}

export class MatrixStack extends Stack {
  readonly targets: readonly MatrixTarget[];

  constructor(scope: Construct, id: string, props: MatrixStackProps) {
    super(scope, id, props);
    if (props.targets.length === 0) {
      throw new Error('a run with no targets bills nothing and tests nothing');
    }
    this.targets = props.targets;

    const network = new MatrixNetwork(this, 'Network');
    // One parameter group per engine family, shared by every row of that family: the settings are
    // the family's, and a second copy of them per instance would buy nothing.
    const parameterGroups = new Map<string, MatrixParameterGroup>();
    for (const target of props.targets) {
      const secret = new MatrixAdminSecret(this, `Admin-${target.id}`);
      const family = parameterFamilyOf(target);
      let parameterGroup = parameterGroups.get(family);
      if (parameterGroup === undefined) {
        parameterGroup = new MatrixParameterGroup(this, `Params-${family.replace(/[^A-Za-z0-9]/g, '-')}`, {
          engine: target.engine,
          engineVersion: target.engineVersion,
        });
        parameterGroups.set(family, parameterGroup);
      }
      new MatrixDbInstance(this, `Db-${target.id}`, {
        vpc: network.vpc,
        subnetGroup: network.databaseSubnetGroup,
        securityGroup: network.databaseSecurityGroup,
        engine: target.engine,
        engineVersion: target.engineVersion,
        instanceType: target.instanceType,
        secret,
        parameterGroup,
      });
    }
  }
}

/**
 * The parameter group family a target's settings belong to. Asked of the engine object rather than
 * spelled out here, so the group and the instance cannot end up configured for different engines.
 */
function parameterFamilyOf(target: MatrixTarget): string {
  const family = instanceEngineFor(target.engine, target.engineVersion).parameterGroupFamily;
  if (family === undefined) {
    throw new Error(`target "${target.id}" runs a build with no parameter group family`);
  }
  return family;
}

export interface MatrixAppProps {
  /** Target ids to deploy. @default every row in RDS_TARGETS */
  readonly targets?: readonly string[];
  readonly expiry: string;
  readonly runId: string;
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
  });
  applyMatrixTags(stack, { expiry: props.expiry, runId: props.runId });
  return stack;
}

function selectTargets(ids?: readonly string[]): MatrixTarget[] {
  if (ids === undefined) {
    return RDS_TARGETS.map((target) => ({ ...target }));
  }
  if (ids.length === 0) {
    throw new Error('a run with no targets bills nothing and tests nothing');
  }
  return ids.map((id) => {
    const target = RDS_TARGETS.find((candidate) => candidate.id === id);
    if (!target) {
      throw new Error(`unknown target "${id}"; the matrix has ${RDS_TARGETS.map((candidate) => candidate.id).join(', ')}`);
    }
    return { ...target };
  });
}

/** Reads a comma-separated context value such as `-c targets=rds-mysql-84,rds-pg-17`. */
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
    env: account && region ? { account, region } : undefined,
  });
}

if (require.main === module) {
  main();
}
