/**
 * The compute a run's suites actually execute on: one Fargate task inside the run's VPC.
 *
 * The suite is not run from the CI runner over a public database endpoint. Admitting GitHub's
 * published Actions ranges to a managed database on 3306/5432 for the length of a run would open
 * exactly the surface this matrix exists to measure the absence of, so the databases stay private
 * and the compute moves to them instead: one task, one vCPU, 3 GiB, in the same isolated subnets
 * the databases are in, reaching them through one security-group rule and nothing else. The runner
 * that orchestrates all of this only synthesizes, deploys, dispatches and collects.
 *
 * What that costs, since an isolated subnet has no road to the internet: the task pulls its image
 * from ECR, registers with the cluster, writes its logs and reads the master credential through
 * interface endpoints — the set below, priced per availability-zone-hour — and reaches the artifact
 * bucket through a free gateway endpoint. The endpoints cost more than the task does, which is the
 * honest price of a private managed database; the alternative that costs nothing is a public
 * endpoint, and that is the thing this composition exists to avoid.
 *
 * Like the rest of this directory: no engine file, no hot path, nothing that runs per binlog
 * event. The task runs the existing suites, so the only writes it makes to a database are the
 * `lvt_*` schemas the harness already creates and drops; the one administrative write a run makes
 * (RDS binlog retention, on the admin account, once, at create time) is a server-administration
 * procedure in the runbook, not engine state, so it has no control store to live in.
 */
import { CfnOutput, RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { MatrixArtifactBucket } from './bucket';
import { MatrixEngine } from './db-instance';
import type { MatrixNetwork } from './network';
import type { MatrixAdminSecret } from './secret';

/** What the plan priced the suite at: one vCPU and 3 GiB, which is the ceiling as well as the floor. */
export const RUNNER_TASK_CPU = 1024;
export const RUNNER_TASK_MEMORY_MIB = 3072;

/** An image the task may run: a pinned ECR build. `latest` and untagged are not builds. */
const IMAGE_URI_PATTERN = /^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com\/[a-z0-9._\-/]+:[A-Za-z0-9._\-]+$/;

/** The scratch the suites write to: the task's own ephemeral layer, gone with the task. */
export const WORK_CONTAINER_PATH = '/var/tmp/matrix';

/** The unprivileged uid/gid the container runs as (`nobody`): the suites are untrusted input. */
export const CONTAINER_USER = '65534:65534';

/** Prefix of the log stream the task's own output is kept under. */
export const LOG_STREAM_PREFIX = 'matrix';

/** The engine writes these whether asked or not; without them a failed task has no output at all. */
const LOG_RETENTION = logs.RetentionDays.ONE_WEEK;

/**
 * The endpoints an isolated Fargate task needs: ECR to pull, the ECS control plane and agent to
 * register and report, CloudWatch Logs for the log driver (and the metrics behind it), Secrets
 * Manager for the credential, and the two messaging endpoints the managed agent's channel uses.
 * The S3 gateway endpoint is separate: it is free and carries the artifacts.
 */
const REQUIRED_INTERFACE_ENDPOINTS: ReadonlyArray<readonly [string, ec2.InterfaceVpcEndpointAwsService]> = [
  ['EcrApi', ec2.InterfaceVpcEndpointAwsService.ECR],
  ['EcrDocker', ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER],
  ['Ecs', ec2.InterfaceVpcEndpointAwsService.ECS],
  ['EcsAgent', ec2.InterfaceVpcEndpointAwsService.ECS_AGENT],
  ['EcsTelemetry', ec2.InterfaceVpcEndpointAwsService.ECS_TELEMETRY],
  ['CloudWatchLogs', ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS],
  ['CloudWatchMonitoring', ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_MONITORING],
  ['SecretsManager', ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER],
  ['SsmMessages', ec2.InterfaceVpcEndpointAwsService.SSM_MESSAGES],
  ['Ec2Messages', ec2.InterfaceVpcEndpointAwsService.EC2_MESSAGES],
];

/** The names above, for the test that counts them: an unlisted endpoint is a cost nobody agreed to. */
export const REQUIRED_INTERFACE_ENDPOINT_NAMES: readonly string[] = REQUIRED_INTERFACE_ENDPOINTS.map(([name]) => name);

/** Published per-availability-zone-hour price of an interface endpoint in the regions the matrix runs in. */
export const INTERFACE_ENDPOINT_USD_PER_AZ_HOUR = 0.01;

/** What the endpoints of one run cost, so the note above cannot drift away from the resources. */
export function endpointCostUsd(azs: number, hours: number): number {
  return Number((REQUIRED_INTERFACE_ENDPOINTS.length * azs * hours * INTERFACE_ENDPOINT_USD_PER_AZ_HOUR).toFixed(4));
}

/** Port a target family's suites connect to, so the task opens exactly that and nothing else. */
export function defaultDatabasePort(engine: MatrixEngine): number {
  return engine === MatrixEngine.PostgreSQL ? 5432 : 3306;
}

/**
 * One database of the run, as the task is told about it: which row it is, what protocol it speaks,
 * and where its master credential lives. The id becomes the environment variable name the task's
 * entrypoint reads, so a target's credential can never be handed to the wrong host.
 */
export interface MatrixDatabaseCredential {
  readonly id: string;
  readonly engine: MatrixEngine;
  readonly secret: MatrixAdminSecret;
}

/** `rds-pg-17` and `rds_pg_17` name the same row, so they must not produce two variables. */
export function credentialEnvironmentName(id: string): string {
  const name = `MATRIX_CREDENTIAL_${id.toUpperCase().replace(/[^A-Z0-9]+/g, '_')}`;
  // The id has to survive as a name: `''` and `--` would all collapse to the same suffix, and two
  // credentials under one variable is one credential handed to the wrong host.
  if (!/^MATRIX_CREDENTIAL_[A-Z0-9]+(_[A-Z0-9]+)*$/.test(name)) {
    throw new Error(`target id "${id}" cannot name an environment variable`);
  }
  return name;
}

/** How the task is launched. `DISABLED` is the only value the composition will state. */
export const ASSIGN_PUBLIC_IP = 'DISABLED' as const;

export interface MatrixTaskNetworkConfiguration {
  readonly subnets: readonly string[];
  readonly securityGroups: readonly string[];
  readonly assignPublicIp: typeof ASSIGN_PUBLIC_IP;
}

export interface MatrixRunnerProps {
  /** The run's network: the isolated subnets, and the database group the task is allowed to reach. */
  readonly network: MatrixNetwork;
  /** The run's artifact bucket: binaries in, logs out. */
  readonly bucket: MatrixArtifactBucket;
  /** The databases of the run, in target order: one credential each, one port per family. */
  readonly databases: readonly MatrixDatabaseCredential[];
  /** Pinned ECR URI of the runner image, `account.dkr.ecr.region.amazonaws.com/repo:tag`. */
  readonly imageUri: string;
  /** Ties the task's log stream and artifact prefix to one workflow run. */
  readonly runId: string;
  /** @default RUNNER_TASK_CPU, and never more */
  readonly cpu?: number;
  /** @default RUNNER_TASK_MEMORY_MIB, and never more */
  readonly memoryLimitMiB?: number;
}

export class MatrixRunner extends Construct {
  readonly cluster: ecs.Cluster;
  readonly taskDefinition: ecs.FargateTaskDefinition;
  readonly taskSecurityGroup: ec2.ISecurityGroup;
  /** The endpoint id the bucket's reads are restricted to, for the record and for assertions. */
  readonly s3EndpointId: string;

  constructor(scope: Construct, id: string, props: MatrixRunnerProps) {
    super(scope, id);
    if (!IMAGE_URI_PATTERN.test(props.imageUri)) {
      throw new Error(`runner image "${props.imageUri}" is not a pinned ECR build (12-digit account, region, repo and an explicit tag)`);
    }
    if (props.imageUri.endsWith(':latest')) {
      throw new Error('the runner image must be a build, not :latest: a run has to be reproducible from its own log');
    }
    if (props.databases.length === 0) {
      throw new Error('a task with no databases to test has nothing to run and no port to be allowed');
    }
    const ids = props.databases.map((database) => credentialEnvironmentName(database.id));
    if (new Set(ids).size !== ids.length) {
      throw new Error(`two target ids name the same credential variable (${ids.join(', ')}): one credential would be handed to the wrong host`);
    }
    const cpu = props.cpu ?? RUNNER_TASK_CPU;
    const memoryLimitMiB = props.memoryLimitMiB ?? RUNNER_TASK_MEMORY_MIB;
    if (cpu < 256 || cpu > RUNNER_TASK_CPU || memoryLimitMiB < 512 || memoryLimitMiB > RUNNER_TASK_MEMORY_MIB) {
      throw new Error(`the run's compute is capped at ${RUNNER_TASK_CPU} vCPU-units and ${RUNNER_TASK_MEMORY_MIB} MiB (got ${cpu}/${memoryLimitMiB}); a suite that needs more is a plan change, not a prop`);
    }
    if (!/^[A-Za-z0-9_.-]+$/.test(props.runId)) {
      throw new Error(`run id "${props.runId}" cannot be used in a log stream name`);
    }

    const vpc = props.network.vpc;
    const isolated: ec2.SubnetSelection = { subnetType: ec2.SubnetType.PRIVATE_ISOLATED };
    this.cluster = new ecs.Cluster(this, 'Cluster', {
      vpc,
      // Fargate and Fargate Spot attached to the cluster: the run is one idempotent task per
      // target, so a spot interruption shows up as that target failing, never as a green row.
      enableFargateCapacityProviders: true,
      // Container Insights is a per-metric bill for a dashboard nobody reads during a run; the
      // suites' own output and the engine logs are what a failure is diagnosed from.
      containerInsights: false,
    });

    // The task starts every conversation and receives none: no ingress rule exists for it, so no
    // port on the run's compute is reachable by anything, including the databases.
    this.taskSecurityGroup = new ec2.SecurityGroup(this, 'TaskSecurityGroup', {
      vpc,
      description: 'The test task of one managed-database test run',
      allowAllOutbound: false,
    });
    // The endpoints answer only the task, and answer nothing else: an outbound rule on them would
    // be a road out of the VPC through the very resources that exist to avoid one.
    const endpointSecurityGroup = new ec2.SecurityGroup(this, 'EndpointSecurityGroup', {
      vpc,
      description: 'VPC endpoints the test task of one run may reach',
      allowAllOutbound: false,
    });
    endpointSecurityGroup.addIngressRule(this.taskSecurityGroup, ec2.Port.tcp(443), 'the task reaches the AWS endpoints over TLS and nothing else');
    // CDK pairs an ingress rule with egress only when it can render both inline. The task starts
    // every conversation and receives none, so its side of each rule is written here: without it
    // the group that may not egress would also not reach the endpoints the task cannot run without.
    this.taskSecurityGroup.addEgressRule(ec2.Peer.securityGroupId(endpointSecurityGroup.securityGroupId), ec2.Port.tcp(443), 'out to the endpoints, by group and port only');

    for (const [name, service] of REQUIRED_INTERFACE_ENDPOINTS) {
      new ec2.InterfaceVpcEndpoint(this, `Endpoint-${name}`, {
        vpc,
        service,
        securityGroups: [endpointSecurityGroup],
        privateDnsEnabled: true,
      });
    }
    const s3Endpoint = new ec2.GatewayVpcEndpoint(this, 'S3Endpoint', {
      vpc,
      service: ec2.GatewayVpcEndpointAwsService.S3,
      subnets: [isolated],
    });
    this.s3EndpointId = s3Endpoint.vpcEndpointId;
    props.bucket.denyReadsOutsideVpc(this.s3EndpointId);

    this.taskDefinition = new ecs.FargateTaskDefinition(this, 'Task', {
      cpu,
      memoryLimitMiB,
      // The binaries are built on the CI runner's architecture, so the platform is stated rather
      // than left to a default: a task of another one could not execute them.
      runtimePlatform: {
        cpuArchitecture: ecs.CpuArchitecture.X86_64,
        operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
      },
    });

    const logGroup = new logs.LogGroup(this, 'TaskLogs', {
      logGroupName: `/ecs/${LOG_STREAM_PREFIX}/${props.runId}`,
      retention: LOG_RETENTION,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    this.taskDefinition.addContainer('Suite', {
      image: ecs.ContainerImage.fromRegistry(props.imageUri),
      logging: new ecs.AwsLogDriver({ streamPrefix: LOG_STREAM_PREFIX, logGroup }),
      // No privilege and no root user: the suites are untrusted input to a machine that can reach
      // the databases. The task keeps a writable scratch (the suites need one, and a tmpfs of this
      // CDK release would have taken it out of the 3 GiB of memory), but that scratch is the task's
      // own ephemeral layer: it is not a volume, nothing is mounted into it from outside, and it
      // disappears with the task. `Volumes` is asserted absent for exactly that reason.
      privileged: false,
      user: CONTAINER_USER,
      environment: {
        MATRIX_RUN_ID: props.runId,
        MATRIX_ARTIFACT_BUCKET: props.bucket.bucket.bucketName,
        MATRIX_WORK_PATH: WORK_CONTAINER_PATH,
      },
      // The master credentials arrive as Secrets Manager references, one per target, so no password
      // is ever a string in the task definition, in a log, or in the runner's environment.
      secrets: Object.fromEntries(
        props.databases.map((database) => [credentialEnvironmentName(database.id), ecs.Secret.fromSecretsManager(database.secret.secret)]),
      ),
    });

    // Read the binaries, write the artifacts, read one credential per database the run deploys: the
    // task role gets nothing else, and in particular no RDS or IAM action — it tests the databases,
    // it does not administer them.
    props.bucket.grantReadBinaries(this.taskDefinition.taskRole);
    props.bucket.grantWriteArtifacts(this.taskDefinition.taskRole);
    for (const database of props.databases) {
      database.secret.secret.grantRead(this.taskDefinition.taskRole);
    }

    // One rule per engine family the run actually deploys, from the task's group to the databases'
    // group. No CIDR appears: the only thing that may reach a database is this run's task.
    const ports = [...new Set(props.databases.map((database) => defaultDatabasePort(database.engine)))].sort();
    for (const port of ports) {
      props.network.allowDatabasesFrom(this.taskSecurityGroup, ec2.Port.tcp(port), `the task drives the suite over the ${port} protocol`);
      // The task's half of the same rule, or the suite would open nothing: see the note above.
      this.taskSecurityGroup.addEgressRule(
        ec2.Peer.securityGroupId(props.network.databaseSecurityGroup.securityGroupId),
        ec2.Port.tcp(port),
        `the task drives the suite over the ${port} protocol`,
      );
    }

    // The launch configuration is exported rather than written down in a runbook: a workflow that
    // ran the task somewhere else would have to contradict these values on purpose.
    const subnets = vpc.selectSubnets(isolated).subnetIds;
    new CfnOutput(this, 'TaskSubnetIds', { value: subnets.join(','), description: 'The isolated subnets the task runs in' });
    new CfnOutput(this, 'TaskSecurityGroupId', { value: this.taskSecurityGroup.securityGroupId, description: 'The only source a database accepts the suite port from' });
    new CfnOutput(this, 'TaskAssignPublicIp', { value: ASSIGN_PUBLIC_IP, description: 'A task with a public address is not what this composition builds' });
    new CfnOutput(this, 'TaskClusterName', { value: this.cluster.clusterName, description: 'The cluster the run dispatches its task to' });
    new CfnOutput(this, 'ArtifactBucketName', { value: props.bucket.bucket.bucketName, description: 'Binaries in from the runner, artifacts out from the task' });
  }

  /** The exact network configuration a run must launch the task with. Public addresses: disabled. */
  get taskNetworkConfiguration(): MatrixTaskNetworkConfiguration {
    return {
      subnets: this.cluster.vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_ISOLATED }).subnetIds,
      securityGroups: [this.taskSecurityGroup.securityGroupId],
      assignPublicIp: ASSIGN_PUBLIC_IP,
    };
  }
}
