/**
 * Offline proof of the in-VPC runner: what the task may reach, what may reach it, and what a run
 * must not be able to do at all. Run with `npm test`; no account, no credentials, no network.
 *
 * The claims are the acceptance criteria, and most of them are claims about absence: no public
 * address, no rule against a range of addresses, no ingress to the task, no RDS or IAM permission
 * on the task role, no object that outlives the run. The interfaces the task needs in order to
 * reach AWS at all are asserted by count and by name, so the price of keeping the databases private
 * is visible in the template rather than assumed.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { App, Duration } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';

import { createMatrixStack, RDS_TARGETS } from '../bin/app';
import { ARTIFACTS_PREFIX, BINARIES_PREFIX, MAX_OBJECT_LIFETIME, MatrixArtifactBucket } from '../lib/bucket';
import { MatrixEngine } from '../lib/db-instance';
import { MatrixNetwork } from '../lib/network';
import {
  ASSIGN_PUBLIC_IP,
  CONTAINER_USER,
  LOG_STREAM_PREFIX,
  MatrixRunner,
  REQUIRED_INTERFACE_ENDPOINT_NAMES,
  RUNNER_TASK_CPU,
  RUNNER_TASK_MEMORY_MIB,
  WORK_CONTAINER_PATH,
  credentialEnvironmentName,
  defaultDatabasePort,
  endpointCostUsd,
} from '../lib/runner';
import { MatrixAdminSecret } from '../lib/secret';
import { EXPIRY_TAG_KEY, MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE, RUN_ID_TAG_KEY } from '../lib/tags';

const RUN_ID = 'test-run';
const IMAGE = '111122223333.dkr.ecr.us-east-1.amazonaws.com/cloud-db-matrix:2026-11-19';
/** How CDK says "this group egresses nothing": an unmatchable host route, not a range. */
const DENY_ALL = 'Disallow all traffic';

type Rule = Record<string, unknown>;
type NamedRule = Rule & { group: string };

function futureExpiry(): string {
  return new Date(Date.now() + 60 * 60 * 1000).toISOString();
}

function synth(targets?: readonly string[], runnerImage: string | undefined = IMAGE): Template {
  return Template.fromStack(createMatrixStack(new App(), { targets, expiry: futureExpiry(), runId: RUN_ID, runnerImage }));
}

/** A run that asked for databases and never named an image: the databases only, no compute at all. */
function synthDatabasesOnly(targets?: readonly string[]): Template {
  return Template.fromStack(createMatrixStack(new App(), { targets, expiry: futureExpiry(), runId: RUN_ID }));
}

function propertiesOf(template: Template, type: string): Array<Record<string, unknown>> {
  return Object.values(template.findResources(type)).map((r) => (r as { Properties: Record<string, unknown> }).Properties);
}

function groupIds(template: Template, fragment: RegExp): string[] {
  return Object.keys(template.findResources('AWS::EC2::SecurityGroup')).filter((id) => fragment.test(id));
}

/** The one VPC of the run, by logical id: rules name its own block rather than a written-down range. */
function expectVpc(template: Template): string {
  const ids = Object.keys(template.findResources('AWS::EC2::VPC'));
  assert.equal(ids.length, 1, 'one VPC for the run');
  return ids[0];
}

function groupAddress(logicalId: string): unknown {
  return { 'Fn::GetAtt': [logicalId, 'GroupId'] };
}

/**
 * Every ingress rule in the run, inline or standalone: CloudFormation allows both spellings and
 * CDK uses the standalone one whenever the peer is another group.
 */
function ingressRules(template: Template): NamedRule[] {
  const rules: NamedRule[] = [];
  for (const [id, resource] of Object.entries(template.findResources('AWS::EC2::SecurityGroup'))) {
    const inline = (resource as { Properties: Record<string, unknown> }).Properties.SecurityGroupIngress as Rule[] | undefined;
    for (const rule of inline ?? []) {
      rules.push({ ...rule, group: id });
    }
  }
  for (const rule of propertiesOf(template, 'AWS::EC2::SecurityGroupIngress')) {
    rules.push({ ...rule, group: (rule.GroupId as { 'Fn::GetAtt': [string, string] })['Fn::GetAtt'][0] });
  }
  return rules;
}

function egressRules(template: Template): NamedRule[] {
  const rules: NamedRule[] = [];
  for (const [id, resource] of Object.entries(template.findResources('AWS::EC2::SecurityGroup'))) {
    const inline = (resource as { Properties: Record<string, unknown> }).Properties.SecurityGroupEgress as Rule[] | undefined;
    for (const rule of inline ?? []) {
      rules.push({ ...rule, group: id });
    }
  }
  for (const rule of propertiesOf(template, 'AWS::EC2::SecurityGroupEgress')) {
    rules.push({ ...rule, group: (rule.GroupId as { 'Fn::GetAtt': [string, string] })['Fn::GetAtt'][0] });
  }
  return rules;
}

const ofGroup = (rules: NamedRule[], group: string) => rules.filter((rule) => rule.group === group);
const isDenyAll = (rule: Rule) => rule.Description === DENY_ALL;
const ports = (rules: NamedRule[]) => rules.map((rule) => Number(rule.FromPort)).sort((a, b) => a - b);

function output(template: Template, name: string): { Value: unknown; Description?: string } {
  const found = Object.entries((template.toJSON() as { Outputs?: Record<string, unknown> }).Outputs ?? {})
    .find(([logicalId]) => logicalId.includes(name));
  assert.ok(found, `the stack exports ${name}`);
  return found[1] as { Value: unknown };
}

const [MYSQL, MARIADB, POSTGRES] = [MatrixEngine.MySQL, MatrixEngine.MariaDB, MatrixEngine.PostgreSQL];

test('the run gets one Fargate task of the pinned build, and it cannot touch the host', () => {
  const template = synth(['rds-mysql-84', 'rds-pg-17']);
  template.resourceCountIs('AWS::ECS::TaskDefinition', 1);
  const [task] = propertiesOf(template, 'AWS::ECS::TaskDefinition');
  assert.deepEqual(task.RequiresCompatibilities, ['FARGATE']);
  assert.equal(task.NetworkMode, 'awsvpc', 'the task has its own interface: it is not sharing another task’s address');
  assert.equal(task.Cpu, String(RUNNER_TASK_CPU));
  assert.equal(task.Memory, String(RUNNER_TASK_MEMORY_MIB));
  assert.deepEqual(task.RuntimePlatform, { CpuArchitecture: 'X86_64', OperatingSystemFamily: 'LINUX' }, 'the binaries are built for this platform');
  assert.equal(task.Volumes, undefined, 'the task mounts no volume: its scratch is the ephemeral layer it dies with');

  const container = (task.ContainerDefinitions as Array<Record<string, unknown>>)[0];
  assert.equal(container.Privileged, false, 'the suites are untrusted input to a machine that can reach the databases');
  assert.equal(container.User, CONTAINER_USER, 'the container runs as nobody, not root');
  assert.equal(container.Image, IMAGE);
  assert.equal(container.Essential, true);
  assert.equal(container.MountPoints, undefined, 'nothing is mounted into the container from outside the task');

  const logging = container.LogConfiguration as { LogDriver: string; Options: Record<string, unknown> };
  assert.equal(logging.LogDriver, 'awslogs');
  assert.equal(logging.Options['awslogs-stream-prefix'], LOG_STREAM_PREFIX);
  assert.match(JSON.stringify(logging.Options['awslogs-group']), /RunnerTaskLogs/, 'the task writes to a group the run owns and destroys');

  // Credentials are never plain environment: they arrive as one Secrets Manager reference per row.
  const environment = new Map((container.Environment as Array<{ Name: string; Value: string }>).map((e) => [e.Name, e.Value]));
  for (const [name, value] of environment) {
    assert.doesNotMatch(`${name}=${value}`, /PASS|SECRET|TOKEN|PASSWORD/i, `${name} must not carry a credential`);
  }
  assert.equal(environment.get('MATRIX_WORK_PATH'), WORK_CONTAINER_PATH, 'the suites are told where the ephemeral scratch is');
  const secrets = container.Secrets as Array<{ Name: string; ValueFrom: unknown }>;
  assert.deepEqual(secrets.map((secret) => secret.Name).sort(), [credentialEnvironmentName('rds-mysql-84'), credentialEnvironmentName('rds-pg-17')]);
  for (const [index, secret] of secrets.entries()) {
    const id = index === 0 ? 'rds-mysql-84' : 'rds-pg-17';
    assert.match(JSON.stringify(secret.ValueFrom), new RegExp(`Admin${id.replace(/[^a-z0-9]/gi, '')}Admin`), `${secret.Name} points at its own database's credential`);
  }
});

test('no task of this run can have a public address, and the stack says so', () => {
  const template = synth();
  for (const found of renderedAssignPublicIp(template)) {
    assert.equal(found, 'DISABLED', 'a task launched with a public address is not what this composition builds');
  }
  for (const never of ['AWS::EC2::InternetGateway', 'AWS::EC2::NatGateway', 'AWS::EC2::EIP', 'AWS::ElasticLoadBalancingV2::LoadBalancer']) {
    template.resourceCountIs(never, 0);
  }
  for (const subnet of propertiesOf(template, 'AWS::EC2::Subnet')) {
    assert.notEqual(subnet.MapPublicIpOnLaunch, true);
  }

  assert.equal(output(template, 'TaskAssignPublicIp').Value, ASSIGN_PUBLIC_IP);
  // The launch configuration is in the template, not in a runbook: the subnets it names are the
  // ones the databases live in, and the only security group it names is the task's.
  const listed = JSON.stringify(output(template, 'TaskSubnetIds').Value);
  const dbSubnetIds = propertiesOf(template, 'AWS::RDS::DBSubnetGroup')[0].SubnetIds as unknown[];
  assert.equal(dbSubnetIds.length, 2);
  for (const subnet of dbSubnetIds) {
    assert.ok(listed.includes(JSON.stringify(subnet)), 'the task and the databases are in the same subnets');
  }
  assert.deepEqual(output(template, 'TaskSecurityGroupId').Value, groupAddress(groupIds(template, /RunnerTaskSecurityGroup/)[0]));
  assert.match(JSON.stringify(output(template, 'ArtifactBucketName').Value), /ArtifactsBucket/);
});

/** Every `AssignPublicIp` the template states, so a single ENABLED anywhere cannot hide. */
function renderedAssignPublicIp(template: Template): string[] {
  return (JSON.stringify(template.toJSON()).match(/"AssignPublicIp":"([^"]*)"/g) ?? []).map((hit) => hit.split(':')[1].replace(/"/g, ''));
}

test('the databases take the suite port from the task, and the task gets nothing back', () => {
  const template = synth(['rds-mysql-84', 'rds-pg-17']);
  const [dbGroup] = groupIds(template, /NetworkDatabaseSecurityGroup/);
  const [taskGroup] = groupIds(template, /RunnerTaskSecurityGroup/);
  const [endpointGroup] = groupIds(template, /RunnerEndpointSecurityGroup/);
  assert.ok(dbGroup && taskGroup && endpointGroup, 'one group each for the databases, the task and the endpoints');

  const ingress = ingressRules(template);
  const dbIngress = ofGroup(ingress, dbGroup);
  assert.deepEqual(ports(dbIngress), [3306, 5432], 'one rule per engine family the run deploys');
  for (const rule of dbIngress) {
    assert.deepEqual(rule.SourceSecurityGroupId, groupAddress(taskGroup), 'the task is the only source');
    assert.equal(rule.IpProtocol, 'tcp');
    assert.equal(rule.FromPort, rule.ToPort);
    for (const range of ['CidrIp', 'CidrIpv6', 'PrefixListId', 'SourcePrefixListId']) {
      assert.equal(rule[range], undefined, `a database reached by range is reached by whoever holds it: ${range}`);
    }
  }
  // A postgres-only run must not leave 3306 open, and a mysql-only run must not leave 5432 open.
  const pgOnly = synth(['rds-pg-17']);
  assert.deepEqual(ports(ofGroup(ingressRules(pgOnly), groupIds(pgOnly, /NetworkDatabaseSecurityGroup/)[0])), [defaultDatabasePort(POSTGRES)]);
  const mysqlOnly = synth(['rds-mariadb-114']);
  assert.deepEqual(ports(ofGroup(ingressRules(mysqlOnly), groupIds(mysqlOnly, /NetworkDatabaseSecurityGroup/)[0])), [defaultDatabasePort(MARIADB)]);

  // Nothing reaches the task at all; it starts three conversations and receives none.
  assert.deepEqual(ofGroup(ingress, taskGroup), []);
  const taskEgress = ofGroup(egressRules(template), taskGroup).filter((rule) => !isDenyAll(rule));
  assert.deepEqual(ports(taskEgress), [443, 3306, 5432], 'the task reaches the endpoints and the databases, by group and port');
  for (const rule of taskEgress) {
    assert.ok(rule.DestinationSecurityGroupId, `an egress rule of the task must name a group, not a range: ${JSON.stringify(rule)}`);
  }
  // The endpoints answer 443 from inside the run's own VPC and from the task; they egress nothing,
  // because an endpoint that could egress would be a road out through the resources that exist to
  // keep the run from needing one.
  const endpointIngress = ofGroup(ingress, endpointGroup).filter((rule) => !isDenyAll(rule));
  assert.deepEqual(ports(endpointIngress), [443, 443], 'one port in, the TLS port of the AWS APIs, from exactly two sources');
  const endpointSources = endpointIngress.map((rule) => JSON.stringify(rule.SourceSecurityGroupId ?? rule.CidrIp)).sort();
  assert.deepEqual(endpointSources, [JSON.stringify(groupAddress(taskGroup)), JSON.stringify({ 'Fn::GetAtt': [expectVpc(template), 'CidrBlock'] })].sort(), 'the task’s group, and this VPC’s own block by reference: never a written-down range');
  for (const group of [dbGroup, endpointGroup]) {
    for (const rule of ofGroup(egressRules(template), group)) {
      assert.ok(isDenyAll(rule), `${group} must start no conversations of its own`);
    }
  }

  for (const rule of [...ingress, ...egressRules(template)]) {
    for (const field of ['CidrIp', 'CidrIpv6']) {
      const value = rule[field];
      if (typeof value === 'string') {
        assert.ok(isDenyAll(rule), `${JSON.stringify(rule)} is a rule against a written-down range`);
        assert.equal(value, '255.255.255.255/32', 'the only literal range in the run is CDK’s own "egress nothing" placeholder');
      }
    }
  }
});

test('the task reaches AWS only through the endpoints of its own run', () => {
  const template = synth();
  const [taskGroup] = groupIds(template, /RunnerTaskSecurityGroup/);
  const [endpointGroup] = groupIds(template, /RunnerEndpointSecurityGroup/);

  const endpoints = propertiesOf(template, 'AWS::EC2::VPCEndpoint');
  const interfaces = endpoints.filter((endpoint) => endpoint.VpcEndpointType === 'Interface');
  assert.equal(interfaces.length, REQUIRED_INTERFACE_ENDPOINT_NAMES.length, 'every endpoint is named in the composition, so its price is visible');
  const dbSubnets = propertiesOf(template, 'AWS::RDS::DBSubnetGroup')[0].SubnetIds as unknown[];
  for (const endpoint of interfaces) {
    assert.equal(endpoint.PrivateDnsEnabled, true);
    assert.deepEqual(endpoint.SecurityGroupIds, [groupAddress(endpointGroup)], 'the endpoints answer through one group');
    assert.notEqual(JSON.stringify(endpoint.SecurityGroupIds).includes(taskGroup), true, 'the task group is not the endpoint’s door');
    assert.deepEqual(endpoint.SubnetIds, dbSubnets, 'the endpoints live where the task and the databases do');
  }
  const gateway = endpoints.filter((endpoint) => endpoint.VpcEndpointType === 'Gateway');
  assert.equal(gateway.length, 1, 'the artifacts go to S3 over the free gateway endpoint');
  assert.match(JSON.stringify(gateway[0].ServiceName), /com\.amazonaws\./);
  assert.match(JSON.stringify(gateway[0].ServiceName), /"\.s3"/, 'the service is S3 itself, named by the region the run deploys in');
  assert.equal(gateway[0].SecurityGroupIds, undefined, 'a gateway endpoint has no door at all: it is a route');

  template.resourceCountIs('AWS::ECS::Cluster', 1);
  const associations = propertiesOf(template, 'AWS::ECS::ClusterCapacityProviderAssociations');
  assert.equal(associations.length, 1);
  assert.match(JSON.stringify(associations[0].CapacityProviders), /FARGATE_SPOT/, 'a spot interruption fails a row, it never greens one');
  const [cluster] = propertiesOf(template, 'AWS::ECS::Cluster');
  const settings = (cluster.ClusterSettings ?? []) as Array<{ Name: string; Value: string }>;
  const insights = settings.find((setting) => setting.Name === 'containerInsights');
  assert.ok(insights === undefined || insights.Value === 'disabled', `Container Insights is a per-metric bill for a dashboard nobody reads mid-run (got ${insights?.Value})`);

  // The endpoints are the price of a private database, and the note states it as arithmetic.
  assert.equal(endpointCostUsd(2, 3), Number((REQUIRED_INTERFACE_ENDPOINT_NAMES.length * 2 * 3 * 0.01).toFixed(4)));
  assert.equal(defaultDatabasePort(MYSQL), 3306);
  assert.equal(defaultDatabasePort(POSTGRES), 5432);
});

test('the artifact bucket is private, encrypted, expiring, destroyed, and readable only from inside', () => {
  const template = synth(['rds-mysql-84']);
  template.resourceCountIs('AWS::S3::Bucket', 1);
  const [bucket] = propertiesOf(template, 'AWS::S3::Bucket');
  assert.deepEqual(bucket.PublicAccessBlockConfiguration, {
    BlockPublicAcls: true,
    BlockPublicPolicy: true,
    IgnorePublicAcls: true,
    RestrictPublicBuckets: true,
  });
  const encryption = (bucket.BucketEncryption as { ServerSideEncryptionConfiguration: Array<{ ServerSideEncryptionByDefault: Record<string, unknown> }> }).ServerSideEncryptionConfiguration;
  assert.equal(encryption[0].ServerSideEncryptionByDefault.SSEAlgorithm, 'aws:kms');
  assert.match(JSON.stringify(encryption[0].ServerSideEncryptionByDefault.KMSMasterKeyID), /ArtifactsBucketKey/, 'a key the run owns, by reference');
  assert.deepEqual((bucket.OwnershipControls as { Rules: unknown[] }).Rules, [{ ObjectOwnership: 'BucketOwnerEnforced' }]);
  const rule = (bucket.LifecycleConfiguration as { Rules: Array<Record<string, unknown>> }).Rules[0];
  assert.ok((rule.ExpirationInDays as number) <= MAX_OBJECT_LIFETIME.toDays(), 'objects die with the run');
  assert.ok(rule.AbortIncompleteMultipartUpload, 'a stalled upload is not a reason to pay for parts');
  for (const [logicalId, resource] of Object.entries(template.findResources('AWS::S3::Bucket'))) {
    assert.equal((resource as { DeletionPolicy?: string }).DeletionPolicy, 'Delete', `${logicalId} must not survive the run`);
    const tags = (resource as { Properties: { Tags: Array<{ Key: string; Value: string }> } }).Properties.Tags;
    const byKey = new Map(tags.map((tag) => [tag.Key, tag.Value]));
    for (const key of [EXPIRY_TAG_KEY, RUN_ID_TAG_KEY, MANAGED_BY_TAG_KEY]) {
      assert.ok(byKey.has(key), `${logicalId} is missing the ${key} tag`);
    }
    assert.equal(byKey.get(MANAGED_BY_TAG_KEY), MANAGED_BY_TAG_VALUE, 'the key the owner budgets on');
    assert.equal(byKey.get(RUN_ID_TAG_KEY), RUN_ID, 'the tag that ties the bill to one run');
  }
  // Teardown must not be blocked by an object that arrived after the last lifecycle sweep.
  template.resourceCountIs('Custom::S3AutoDeleteObjects', 1);

  const policy = propertiesOf(template, 'AWS::S3::BucketPolicy')[0].PolicyDocument as { Statement: Array<Rule> };
  const denies = policy.Statement.filter((statement) => statement.Effect === 'Deny' && JSON.stringify(statement.Action).includes('s3:GetObject'));
  assert.equal(denies.length, 1, 'reads are served from inside the VPC only');
  assert.deepEqual(denies[0].Principal, { AWS: '*' }, 'the deny covers every principal, including this account');
  assert.match(JSON.stringify(denies[0].Condition), /aws:SourceVpce/);
  assert.match(JSON.stringify(denies[0].Condition), /S3Endpoint/, 'the policy names the gateway endpoint of this run, not some endpoint');
  for (const statement of policy.Statement.filter((s) => s.Effect === 'Deny')) {
    assert.doesNotMatch(JSON.stringify(statement.Action), /PutObject|DeleteObject/, 'a bucket that cannot be written or emptied cannot be seeded or destroyed');
  }
});

test('the task role reads binaries, writes artifacts, and administers nothing', () => {
  const template = synth(['rds-mysql-84', 'rds-pg-17']);
  const [task] = propertiesOf(template, 'AWS::ECS::TaskDefinition');
  const role = (task.TaskRoleArn as { 'Fn::GetAtt': [string, string] })['Fn::GetAtt'][0];
  const executionRole = (task.ExecutionRoleArn as { 'Fn::GetAtt': [string, string] })['Fn::GetAtt'][0];
  assert.notEqual(executionRole, role, 'the role that pulls the image is not the role that runs the suite');

  const policies = template.findResources('AWS::IAM::Policy') as Record<string, { Properties: { PolicyDocument: { Statement: Array<Rule> }; Roles: unknown[] } }>;
  const statements = Object.values(policies)
    .filter((policy) => JSON.stringify(policy.Properties.Roles).includes(`"${role}"`))
    .flatMap((policy) => policy.Properties.PolicyDocument.Statement);
  assert.ok(statements.length >= 3, 'the task role has the grants the task needs');
  const actionsOf = (statement: Rule) => ([] as string[]).concat(statement.Action as string | string[]);
  const allows = (verb: string, needle: RegExp) =>
    statements.some((statement) => actionsOf(statement).some((action) => action.startsWith(verb)) && needle.test(JSON.stringify(statement.Resource)));

  assert.ok(allows('s3:GetObject', new RegExp(`${BINARIES_PREFIX}/\\*`)), 'the task pulls the prebuilt binaries');
  assert.ok(allows('s3:PutObject', new RegExp(`${ARTIFACTS_PREFIX}/\\*`)), 'the task uploads what it collects');
  assert.ok(allows('secretsmanager:Get', /Admin/), 'one credential per database, by reference');
  for (const statement of statements) {
    for (const action of actionsOf(statement)) {
      assert.doesNotMatch(action, /^rds:|^iam:|^ecs:RunTask|^sts:/, `the task tests the databases, it does not administer them (${action})`);
    }
  }
});

test('a run without a pinned image gets databases and no compute at all', () => {
  const template = synthDatabasesOnly(['rds-mysql-84']);
  template.resourceCountIs('AWS::ECS::TaskDefinition', 0);
  template.resourceCountIs('AWS::ECS::Cluster', 0);
  template.resourceCountIs('AWS::S3::Bucket', 0);
  template.resourceCountIs('AWS::EC2::VPCEndpoint', 0);
  assert.equal(groupIds(template, /Runner/).length, 0, 'the database-only run is exactly the run the skeleton test asserts on');
  assert.throws(() => output(template, 'TaskAssignPublicIp'), /the stack exports/);
});

test('a runner refuses a configuration that would make a run unrepeatable or unreachable', () => {
  const stack = createMatrixStack(new App(), { targets: ['rds-mysql-84'], expiry: futureExpiry(), runId: RUN_ID });
  const network = new MatrixNetwork(stack, 'OtherNetwork');
  const bucket = new MatrixArtifactBucket(stack, 'OtherBucket');
  const secret = new MatrixAdminSecret(stack, 'OtherSecret');
  const base = { network, bucket, runId: RUN_ID, imageUri: IMAGE, databases: [{ id: 'rds-mysql-84', engine: MYSQL, secret }] };

  assert.throws(() => new MatrixRunner(stack, 'Latest', { ...base, imageUri: IMAGE.replace(/:[\w.-]+$/, ':latest') }), /must be a build, not :latest/);
  assert.throws(() => new MatrixRunner(stack, 'NoTag', { ...base, imageUri: IMAGE.replace(/:[\w.-]+$/, '') }), /pinned ECR build/);
  assert.throws(() => new MatrixRunner(stack, 'OffSite', { ...base, imageUri: 'example.com/published:1.2.3' }), /pinned ECR build/);
  assert.throws(() => new MatrixRunner(stack, 'Big', { ...base, cpu: RUNNER_TASK_CPU * 2 }), /capped at/);
  assert.throws(() => new MatrixRunner(stack, 'Fat', { ...base, memoryLimitMiB: RUNNER_TASK_MEMORY_MIB * 2 }), /capped at/);
  assert.throws(() => new MatrixRunner(stack, 'Idle', { ...base, databases: [] }), /no databases to test/);
  assert.throws(() => new MatrixRunner(stack, 'OddRun', { ...base, runId: 'not a run id' }), /cannot be used in a log stream name/);
  assert.throws(
    () => new MatrixRunner(stack, 'Collide', { ...base, databases: [...base.databases, { id: 'rds_mysql_84', engine: MYSQL, secret }] }),
    /name the same credential variable/,
  );
  assert.throws(() => new MatrixArtifactBucket(stack, 'Stays', { objectLifetime: Duration.days(4) }), /object lifetime must be between/);
  assert.throws(() => new MatrixArtifactBucket(stack, 'Forever', { objectLifetime: Duration.seconds(0) }), /object lifetime must be between/);
  assert.throws(() => bucket.denyReadsOutsideVpc('not-an-endpoint'), /is not a VPC endpoint id/);
  assert.equal(new MatrixRunner(stack, 'Fine', base).taskNetworkConfiguration.assignPublicIp, ASSIGN_PUBLIC_IP);
  assert.equal(credentialEnvironmentName('rds-pg-17'), 'MATRIX_CREDENTIAL_RDS_PG_17');
  assert.throws(() => credentialEnvironmentName(''), /cannot name an environment variable/);
});

test('every target row of the matrix has a port and a credential name, and no two share one', () => {
  const names = RDS_TARGETS.map((target) => credentialEnvironmentName(target.id));
  assert.equal(new Set(names).size, RDS_TARGETS.length, 'two rows may not share one credential variable');
  for (const target of RDS_TARGETS) {
    assert.ok([3306, 5432].includes(defaultDatabasePort(target.engine)), `${target.id} speaks a port the suites can reach`);
  }
});
