/**
 * Offline proof of the AWS composition: what a run creates, what it never creates, and what it
 * refuses to create at all. Run with `npm test`; it needs no account, no credentials, no network.
 *
 * The assertions are the acceptance criteria, so they say what must be absent as loudly as what
 * must be present: no public endpoint, no route out, no stored password, nothing that survives the
 * run and keeps billing afterwards.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';

import { createMatrixStack, MATRIX_STACK_NAME, RDS_TARGETS } from '../bin/app';
import { DEFAULT_DATABASE_NAME, exportedLogTypes, MatrixDbInstance, MatrixEngine, MIN_BACKUP_RETENTION_DAYS } from '../lib/db-instance';
import { MATRIX_VPC_CIDR, MatrixNetwork } from '../lib/network';
import { ADMIN_PASSWORD_LENGTH, DEFAULT_ADMIN_USERNAME, EXCLUDED_PASSWORD_CHARACTERS, MatrixAdminSecret } from '../lib/secret';
import { EXPIRY_TAG_KEY, MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE, RUN_ID_TAG_KEY } from '../lib/tags';

const RUN_ID = 'test-run';

/** The rule CDK writes instead of an open egress rule when a group may not start conversations. */
const DENY_ALL_EGRESS = 'Disallow all traffic';

/** A run with time left on its clock, so nothing has a reason to refuse it. */
function futureExpiry(): string {
  return new Date(Date.now() + 60 * 60 * 1000).toISOString();
}

function synth(targets?: readonly string[], expiry: string = futureExpiry()): Template {
  return Template.fromStack(createMatrixStack(new App(), { targets, expiry, runId: RUN_ID }));
}

function propertiesOf(template: Template, type: string): Array<Record<string, unknown>> {
  return Object.values(template.findResources(type)).map((r) => (r as { Properties: Record<string, unknown> }).Properties);
}

/**
 * The three matrix tags, in the order the tag manager renders them (alphabetical by key).
 * `Match.arrayWith` walks the actual list once, left to right, so a pattern that comes
 * out of render order runs out of elements to try.
 */
const MATRIX_TAGS = Match.arrayWith([
  { Key: EXPIRY_TAG_KEY, Value: Match.stringLikeRegexp('^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}') },
  { Key: MANAGED_BY_TAG_KEY, Value: MANAGED_BY_TAG_VALUE },
  { Key: RUN_ID_TAG_KEY, Value: RUN_ID },
]);

/** The pieces a CloudFormation `Fn::Join` assembles a name from, with unresolved tokens left as-is. */
function joinedParts(value: unknown): unknown[] {
  const joined = (value as { 'Fn::Join'?: [string, unknown[]] })?.['Fn::Join'];
  assert.ok(Array.isArray(joined), 'the name is assembled from the resource it belongs to');
  return joined![1];
}

/** Every resource type the run creates that CloudFormation can tag. */
const TAGGED_TYPES = [
  'AWS::RDS::DBInstance',
  'AWS::RDS::DBSubnetGroup',
  'AWS::SecretsManager::Secret',
  'AWS::KMS::Key',
  'AWS::EC2::VPC',
  'AWS::EC2::Subnet',
  'AWS::EC2::SecurityGroup',
];

test('every target row is one private, encrypted, disposable instance of its pinned build', () => {
  const template = synth();
  template.resourceCountIs('AWS::RDS::DBInstance', RDS_TARGETS.length);
  template.resourceCountIs('AWS::RDS::DBCluster', 0);

  for (const target of RDS_TARGETS) {
    template.hasResourceProperties('AWS::RDS::DBInstance', {
      Engine: target.engine,
      EngineVersion: target.engineVersion,
      DBInstanceClass: target.instanceType,
      DBName: DEFAULT_DATABASE_NAME,
      PubliclyAccessible: false,
      StorageEncrypted: true,
      StorageType: 'gp3',
      DeletionProtection: false,
      AutoMinorVersionUpgrade: false,
      DeleteAutomatedBackups: true,
      MultiAZ: false,
      // Storage grows nowhere: a run pays for the size it asked for and no more.
      MaxAllocatedStorage: Match.absent(),
      Tags: MATRIX_TAGS,
    });
  }

  // RDS keeps no binlogs without automated backups, so a run without them would test nothing.
  for (const properties of propertiesOf(template, 'AWS::RDS::DBInstance')) {
    const retention = properties.BackupRetentionPeriod as number;
    assert.ok(retention >= MIN_BACKUP_RETENTION_DAYS, `backups must be kept, got ${retention} day(s)`);
    assert.equal(typeof properties.MasterUserPassword, 'object', 'the password must be a reference, not a literal');
    assert.match(JSON.stringify(properties.MasterUserPassword), /resolve:secretsmanager:/);
    assert.equal(properties.MasterUsername, DEFAULT_ADMIN_USERNAME);
  }
});

test('the databases take their credentials from a secret, never from the template', () => {
  const template = synth();
  template.resourceCountIs('AWS::SecretsManager::Secret', RDS_TARGETS.length);
  template.hasResourceProperties('AWS::SecretsManager::Secret', {
    GenerateSecretString: {
      GenerateStringKey: 'password',
      PasswordLength: ADMIN_PASSWORD_LENGTH,
      IncludeSpace: false,
      ExcludeCharacters: EXCLUDED_PASSWORD_CHARACTERS,
      SecretStringTemplate: Match.stringLikeRegexp(`"username":"${DEFAULT_ADMIN_USERNAME}"`),
    },
    KmsKeyId: Match.objectLike({ 'Fn::GetAtt': Match.anyValue() }),
    Tags: MATRIX_TAGS,
  });
});

test('the encryption keys the run owns rotate and serve the service that uses them', () => {
  const template = synth();
  const keys = Object.values(template.findResources('AWS::KMS::Key')) as Array<{ Properties: Record<string, unknown> }>;
  assert.ok(keys.length >= RDS_TARGETS.length * 2, 'one key for storage and one for credentials per database');
  let forStorage = 0;
  let forCredentials = 0;
  for (const key of keys) {
    assert.equal(key.Properties.EnableKeyRotation, true);
    const policy = JSON.stringify(key.Properties.KeyPolicy);
    const servesEngine = /rds\.amazonaws\.com/.test(policy);
    const servesSecrets = /secretsmanager\./.test(policy);
    // A key is either the database's storage or its credential: a key that served both would let
    // the engine read master passwords.
    assert.notEqual(servesEngine, servesSecrets, 'every key serves exactly one of the two services');
    if (servesEngine) {
      forStorage += 1;
    } else {
      forCredentials += 1;
    }
  }
  assert.ok(forStorage >= RDS_TARGETS.length, 'every database has a storage key its engine may read');
  assert.ok(forCredentials >= RDS_TARGETS.length, 'every database has a credential key the secret service may read');
  for (const properties of propertiesOf(template, 'AWS::RDS::DBInstance')) {
    assert.equal(properties.StorageEncrypted, true);
    assert.match(JSON.stringify(properties.KmsKeyId), /Fn::GetAtt/);
  }
});

test('nothing in the VPC can be reached from outside it', () => {
  const template = synth();
  template.hasResourceProperties('AWS::EC2::VPC', { CidrBlock: MATRIX_VPC_CIDR, EnableDnsSupport: true });
  for (const never of [
    'AWS::EC2::InternetGateway',
    'AWS::EC2::NatGateway',
    'AWS::EC2::EIP',
    'AWS::EC2::VPCEndpointService',
    'AWS::ElasticLoadBalancingV2::LoadBalancer',
  ]) {
    template.resourceCountIs(never, 0);
  }

  const subnets = propertiesOf(template, 'AWS::EC2::Subnet');
  assert.equal(subnets.length, 2, 'one subnet per AZ, and an AZ pair is the floor for a subnet group');
  for (const subnet of subnets) {
    assert.notEqual(subnet.MapPublicIpOnLaunch, true, 'no subnet hands out public addresses');
  }

  // No default route anywhere: the isolated subnets have nowhere to send traffic outside the VPC.
  for (const route of propertiesOf(template, 'AWS::EC2::Route')) {
    assert.notEqual(route.DestinationCidrBlock, '0.0.0.0/0');
  }
});

test('the databases sit in a two-AZ private subnet group behind a group that opens nothing', () => {
  const template = synth();
  const groups = propertiesOf(template, 'AWS::RDS::DBSubnetGroup');
  assert.equal(groups.length, 1, 'one subnet group for the run, not one per database');
  assert.equal(groups[0].SubnetIds instanceof Array, true);
  const subnetIds = groups[0].SubnetIds as unknown[];
  assert.equal(subnetIds.length, 2);
  assert.equal(new Set(subnetIds.map((id) => JSON.stringify(id))).size, 2, 'a subnet group spans two AZs');

  const securityGroups = propertiesOf(template, 'AWS::EC2::SecurityGroup');
  assert.equal(securityGroups.length, 1, 'one group for the run, not one per database');
  for (const group of securityGroups) {
    assert.deepEqual(group.SecurityGroupIngress ?? [], [], 'ingress is opened by the caller that needs it, not by default');
    const egress = (group.SecurityGroupEgress ?? []) as Array<Record<string, unknown>>;
    for (const rule of egress) {
      assert.equal(rule.Description, DENY_ALL_EGRESS, 'the databases start no conversations');
      assert.notEqual(rule.CidrIp, '0.0.0.0/0');
    }
  }

  for (const properties of propertiesOf(template, 'AWS::RDS::DBInstance')) {
    const attached = properties.VPCSecurityGroups as unknown[];
    assert.equal(attached.length, 1);
    assert.match(JSON.stringify(attached), /NetworkDatabaseSecurityGroup/);
    assert.match(JSON.stringify(properties.DBSubnetGroupName), /NetworkDatabaseSubnetGroup/);
  }
});

test('the engine logs a run collects are owned by the run and capped', () => {
  const template = synth();
  const expected = RDS_TARGETS.reduce((total, target) => total + exportedLogTypes(target.engine).length, 0);
  const groups = propertiesOf(template, 'AWS::Logs::LogGroup');
  assert.equal(groups.length, expected, 'the logs each engine family exports, for each row');
  const suffixes: string[] = [];
  for (const group of groups) {
    const parts = joinedParts(group.LogGroupName);
    assert.equal(parts[0], '/aws/rds/instance/', 'named the way the engine writes them');
    const suffix = String(parts[parts.length - 1]);
    assert.match(suffix, /^\/(error|slowquery|postgresql)$/, 'only the log types the run asked the engine to export');
    assert.ok(
      parts.slice(1, -1).every((part) => typeof part === 'object'),
      'the group is named after its instance by reference, so it can never be left pointing at another run',
    );
    suffixes.push(suffix);
    assert.equal(group.RetentionInDays, 7, 'a week of engine logs is enough to read a failure');
  }
  assert.ok(suffixes.includes('/postgresql'), 'a postgres row keeps its own log type');
  assert.ok(suffixes.includes('/slowquery'), 'a mysql row keeps its slow-query log');
});

test('nothing that bills outlives the run: every stateful resource is destroyed', () => {
  const template = synth();
  for (const type of [
    'AWS::RDS::DBInstance',
    'AWS::RDS::DBSubnetGroup',
    'AWS::SecretsManager::Secret',
    'AWS::KMS::Key',
    'AWS::Logs::LogGroup',
  ]) {
    const found = template.findResources(type);
    assert.ok(Object.keys(found).length > 0, `${type} is part of a run`);
    for (const [logicalId, resource] of Object.entries(found)) {
      assert.equal((resource as { DeletionPolicy?: string }).DeletionPolicy, 'Delete', `${logicalId} must not be retained`);
    }
  }
});

test('every taggable resource says which run it belongs to and when it may go', () => {
  const template = synth();
  for (const type of TAGGED_TYPES) {
    const found = template.findResources(type);
    assert.ok(Object.keys(found).length > 0, `${type} is part of a run`);
    for (const [logicalId, resource] of Object.entries(found)) {
      const tags = (resource as { Properties: { Tags: Array<{ Key: string; Value: string }> } }).Properties.Tags;
      const keys = new Set(tags.map((tag) => tag.Key));
      for (const key of [EXPIRY_TAG_KEY, RUN_ID_TAG_KEY, MANAGED_BY_TAG_KEY]) {
        assert.ok(keys.has(key), `${logicalId} is missing the ${key} tag`);
      }
      // Nothing else identifies a resource beyond CDK's own names: no product, no owner name. The
      // two aws-cdk: keys are how a Vpc labels its own subnets, and `Name` is CDK's own.
      for (const key of keys) {
        assert.ok(
          [EXPIRY_TAG_KEY, RUN_ID_TAG_KEY, MANAGED_BY_TAG_KEY, 'Name', 'aws-cdk:subnet-name', 'aws-cdk:subnet-type'].includes(key),
          `${logicalId} carries an unexpected tag ${key}`,
        );
      }
      assert.equal(tags.find((tag) => tag.Key === MANAGED_BY_TAG_KEY)?.Value, MANAGED_BY_TAG_VALUE);
      assert.equal(tags.find((tag) => tag.Key === RUN_ID_TAG_KEY)?.Value, RUN_ID);
    }
  }
});

test('a run can be narrowed to named targets, and a name that is not a target is an error', () => {
  const template = synth(['rds-pg-17']);
  const instances = propertiesOf(template, 'AWS::RDS::DBInstance');
  assert.equal(instances.length, 1);
  assert.equal(instances[0].EngineVersion, '17.12');

  assert.throws(() => synth(['rds-mysql-99']), /unknown target "rds-mysql-99"/);
  assert.throws(() => synth([]), /no targets/);
});

test('an expiry in the past, or not an instant at all, deploys nothing', () => {
  assert.throws(() => synth(undefined, new Date(Date.now() - 60_000).toISOString()), /already past/);
  assert.throws(() => synth(undefined, 'next tuesday'), /not an ISO-8601 instant/);
});

test('a database refuses a configuration the service would quietly do something else about', () => {
  const app = new App();
  const stack = createMatrixStack(app, { targets: ['rds-mysql-84'], expiry: futureExpiry(), runId: RUN_ID });
  assert.equal(stack.stackName, MATRIX_STACK_NAME);

  const network = new MatrixNetwork(stack, 'ExtraNetwork');
  const secret = new MatrixAdminSecret(stack, 'ExtraSecret');
  const base = {
    vpc: network.vpc,
    subnetGroup: network.databaseSubnetGroup,
    securityGroup: network.databaseSecurityGroup,
    secret,
    engine: MatrixEngine.MySQL,
    engineVersion: '8.4.13',
    instanceType: 'db.t4g.medium',
  };
  assert.throws(() => new MatrixDbInstance(stack, 'TooLittleStorage', { ...base, allocatedStorage: 5 }), /allocated storage must be a whole number/);
  assert.throws(() => new MatrixDbInstance(stack, 'NoBackups', { ...base, backupRetentionDays: 0 }), /backup retention must be at least/);
  assert.throws(() => new MatrixDbInstance(stack, 'FloatingBuild', { ...base, engineVersion: '8' }), /not a full dotted build/);
  assert.throws(() => new MatrixDbInstance(stack, 'OddClass', { ...base, instanceType: 't4g.medium' }), /is not a full RDS class/);
  assert.throws(() => new MatrixNetwork(stack, 'OneAz', { maxAzs: 1 }), /availability zones/);
});
