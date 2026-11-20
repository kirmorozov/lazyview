/**
 * Offline proof of the failover half of the AWS matrix: the shapes a failover run needs, and the
 * settings that make an Aurora cluster worth reading. No account, no credentials, no network — the
 * assertions read the template `cdk synth` would hand CloudFormation.
 *
 * They are the acceptance criteria for PR A3. What they are guarding against is a failover lane that
 * reports success without having failed anything over: a cluster with no reader to promote, a reader
 * in the writer's own availability zone, a cluster whose binlog is switched off by the default group,
 * a standby nobody asked for. The promotion itself is the GTID plan's scenario
 * (`tests/failover_source.rs`), so nothing here re-tests it — it proves there is something to promote.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { App } from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';

import {
  AURORA_TARGETS,
  createMatrixStack,
  MATRIX_TARGETS,
  MULTI_AZ_TARGETS,
  type MatrixClusterTarget,
  type MatrixTarget,
  RDS_TARGETS,
} from '../bin/app';
import {
  assertClusterParameterGroupFamilyFor,
  clusterEngineFor,
  exportedClusterLogTypes,
  MatrixAuroraCluster,
  MatrixClusterParameterGroup,
} from '../lib/aurora-cluster';
import { DEFAULT_DATABASE_NAME, MIN_BACKUP_RETENTION_DAYS } from '../lib/db-instance';
import { MatrixNetwork } from '../lib/network';
import {
  AURORA_BINLOG_BACKUP_PARAMETER,
  AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER,
  AURORA_ENHANCED_BINLOG_PARAMETER,
  AURORA_REPLICA_READ_CONSISTENCY_PARAMETER,
  AURORA_ROW_IMAGE_FULL,
  auroraClusterParametersFor,
  BINLOG_FORMAT_PARAMETER,
  BINLOG_ORDER_COMMITS_PARAMETER,
  BINLOG_ROW_IMAGE_PARAMETER,
  BINLOG_ROW_VALUE_OPTIONS_NONE,
  BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
  BOOLEAN_OFF,
  BOOLEAN_ON,
  CLUSTER_FORBIDDEN_PARAMETERS,
  ENFORCE_GTID_CONSISTENCY_PARAMETER,
  FORCE_SSL_PARAMETER,
  GTID_MODE_PARAMETER,
  LOG_BIN_COMPRESS_PARAMETER,
  LOGICAL_REPLICATION_PARAMETER,
  MAX_REPLICATION_SLOTS_PARAMETER,
  MAX_WAL_SENDERS_PARAMETER,
  MatrixClusterEngine,
  REPLICATION_CONNECTIONS,
  REQUIRE_SECURE_TRANSPORT_PARAMETER,
} from '../lib/params';
import { DEFAULT_ADMIN_USERNAME, MatrixAdminSecret } from '../lib/secret';
import { failoverZones, MIN_STANDBY_INSTANCES } from '../lib/multiaz';
import { EXPIRY_TAG_KEY, MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE, RUN_ID_TAG_KEY } from '../lib/tags';

const RUN_ID = 'test-run';

/** A run with time left on its clock, so nothing has a reason to refuse it. */
function futureExpiry(): string {
  return new Date(Date.now() + 60 * 60 * 1000).toISOString();
}

function synth(targets?: readonly string[], expiry: string = futureExpiry()): Template {
  return Template.fromStack(createMatrixStack(new App(), { targets, expiry, runId: RUN_ID }));
}

function resourcesOf(template: Template, type: string): Array<[string, Record<string, unknown>]> {
  return Object.entries(template.findResources(type) as Record<string, { Properties: Record<string, unknown> }>).map(
    ([id, resource]) => [id, resource as unknown as Record<string, unknown>],
  );
}

function propertiesOf(template: Template, type: string): Array<Record<string, unknown>> {
  return resourcesOf(template, type).map(([, resource]) => resource.Properties as Record<string, unknown>);
}

/**
 * The logical id a value points at, or nothing when it is not a reference. A run's own resources are
 * named either by `{ Ref: … }` or by `{ 'Fn::GetAtt': [ … ] }`, and a value spelled any other way
 * (a literal string, a `{{resolve:…}}`) came from somewhere outside this stack.
 */
function referencedId(value: unknown): string | undefined {
  const node = value as { Ref?: unknown; 'Fn::GetAtt'?: unknown } | undefined;
  if (typeof node?.Ref === 'string') {
    return node.Ref;
  }
  const attribute = node?.['Fn::GetAtt'];
  if (Array.isArray(attribute) && typeof attribute[0] === 'string') {
    return attribute[0];
  }
  return typeof attribute === 'string' ? attribute.split('.')[0] : undefined;
}

/** The three tags a run's resources carry, read straight out of the template. */
function tagValues(tags: unknown): Record<string, string> {
  const list = tags as Array<{ Key: string; Value: string }>;
  assert.ok(Array.isArray(list), 'the resource renders a tag list');
  return Object.fromEntries(list.map((tag) => [tag.Key, tag.Value]));
}

/** What every resource of a run has to say about which run it belongs to and when it may go. */
function expectedTags(expiry: string): Record<string, string> {
  return { [EXPIRY_TAG_KEY]: expiry, [MANAGED_BY_TAG_KEY]: MANAGED_BY_TAG_VALUE, [RUN_ID_TAG_KEY]: RUN_ID };
}

/** The one cluster of a row: its logical id, its properties, and the whole resource. */
function clusterOfRow(template: Template, row: MatrixClusterTarget): { id: string; properties: Record<string, unknown>; resource: Record<string, unknown> } {
  const found = resourcesOf(template, 'AWS::RDS::DBCluster').filter(
    ([, resource]) =>
      (resource.Properties as Record<string, unknown>).Engine === row.engine &&
      (resource.Properties as Record<string, unknown>).EngineVersion === row.engineVersion,
  );
  assert.equal(found.length, 1, `the matrix has exactly one cluster row named ${row.id}`);
  const [id, resource] = found[0]!;
  return { id, properties: resource.Properties as Record<string, unknown>, resource };
}

/**
 * The nodes of a cluster, found by following the reference each one carries back to the cluster. An
 * instance that is not attached to anything is not a node of this run, and a cluster with no nodes
 * synthesizes into a cluster that has no reader to promote.
 */
function nodesOfCluster(template: Template, clusterId: string): Array<{ id: string; properties: Record<string, unknown> }> {
  return resourcesOf(template, 'AWS::RDS::DBInstance')
    .filter(([, resource]) => referencedId((resource.Properties as Record<string, unknown>).DBClusterIdentifier) === clusterId)
    .map(([id, resource]) => ({ id, properties: resource.Properties as Record<string, unknown> }));
}

/**
 * Which of the region's zones a node is told to start in, as an index: the run never names a zone
 * (the stack has no region to name one in), so a node's zone is a pick out of `Fn::GetAZs`, and two
 * nodes with different indices are in two different zones.
 */
function zoneIndex(value: unknown): number {
  const select = (value as { 'Fn::Select'?: [unknown, unknown] })?.['Fn::Select'];
  assert.ok(Array.isArray(select), 'a node names its zone by picking one of the region\'s zones, not by hard-coding a name');
  assert.deepEqual(select![1], { 'Fn::GetAZs': '' }, 'the zone is picked out of the region the run deploys into');
  const index = Number(select![0]);
  assert.ok(Number.isInteger(index), `the pick is a zone index, got ${String(select![0])}`);
  return index;
}

/** The cluster parameter group a cluster is configured with, found through the reference it carries. */
function groupOfCluster(template: Template, cluster: Record<string, unknown>): Record<string, unknown> {
  const id = referencedId(cluster.DBClusterParameterGroupName);
  assert.ok(id, 'the cluster names its cluster parameter group by reference, so it can only be this run\'s');
  const group = (template.findResources('AWS::RDS::DBClusterParameterGroup') as Record<string, unknown>)[id];
  assert.ok(group, `the cluster parameter group "${id}" the cluster refers to exists in this run`);
  return (group as { Properties: Record<string, unknown> }).Properties;
}

function rowsFor(engine: MatrixClusterEngine): MatrixClusterTarget[] {
  return AURORA_TARGETS.filter((row) => row.engine === engine);
}

/** The family RDS names for a pinned Aurora build. */
function familyOf(row: MatrixClusterTarget): string {
  return row.engine === MatrixClusterEngine.AuroraMySQL ? `aurora-mysql8.${row.engineVersion.split('.')[1]}` : `aurora-postgresql${row.engineVersion.split('.')[0]}`;
}

test('every Aurora row is a cluster with a writer and a reader in a zone of its own', () => {
  const expiry = futureExpiry();
  const template = synth(undefined, expiry);
  template.resourceCountIs('AWS::RDS::DBCluster', AURORA_TARGETS.length);

  for (const row of AURORA_TARGETS) {
    const cluster = clusterOfRow(template, row);
    const nodes = nodesOfCluster(template, cluster.id);
    const wanted = (row.readers ?? MIN_STANDBY_INSTANCES) + 1;
    assert.equal(nodes.length, wanted, `${row.id}: a writer and the readers the row asks for`);

    // The reader is the point of the row: without a second node the scenario promotes nothing.
    const readers = nodes.filter((node) => node.id.includes('Reader'));
    assert.equal(readers.length, wanted - 1, `${row.id}: at least one node is a reader, something to promote`);

    // …and it is somewhere else: same zone means the outage takes both.
    const zones = nodes.map((node) => zoneIndex(node.properties.AvailabilityZone));
    assert.equal(new Set(zones).size, zones.length, `${row.id}: ${zones.join(', ')} — a standby in the writer's zone is a copy, not a failover`);

    for (const node of nodes) {
      assert.equal(node.properties.DBInstanceClass, row.instanceType, `${node.id}: the node is the class the row prices`);
      assert.equal(node.properties.PubliclyAccessible, false, `${node.id}: no node of the run is reachable from outside the VPC`);
      assert.deepEqual(
        tagValues(node.properties.Tags),
        expectedTags(expiry),
        `${node.id}: an untagged node is invisible to cleanup and to the budget alarm`,
      );
    }
  }
});

test('every cluster reads the cluster parameter group of its own family, one group per family', () => {
  const template = synth();
  template.resourceCountIs('AWS::RDS::DBClusterParameterGroup', new Set(AURORA_TARGETS.map(familyOf)).size);
  for (const row of AURORA_TARGETS) {
    const group = groupOfCluster(template, clusterOfRow(template, row).properties);
    assert.equal(group.Family, familyOf(row), `${row.id}: the group configures the wrong family`);
  }
});

test('the Aurora MySQL clusters switch their binlog on and give the fence its proof', () => {
  const template = synth();
  for (const row of rowsFor(MatrixClusterEngine.AuroraMySQL)) {
    const group = groupOfCluster(template, clusterOfRow(template, row).properties);
    const parameters = group.Parameters as Record<string, string>;
    // The one parameter that makes the cluster a binlog source: Aurora's `log-bin` is not settable
    // on its own and follows this one, whose family default is OFF.
    assert.equal(parameters[BINLOG_FORMAT_PARAMETER], 'ROW', `${row.id}: a cluster with the binlog off has nothing for the suite to read`);
    // The family spells its row-image values in lower case; the server reports FULL either way and
    // the engine reads the server's value, so only the family's own spelling is accepted here.
    assert.equal(parameters[BINLOG_ROW_IMAGE_PARAMETER], AURORA_ROW_IMAGE_FULL, `${row.id}: a partial row image is not the row a view was built from`);
    assert.equal(parameters[BINLOG_ROW_VALUE_OPTIONS_PARAMETER], BINLOG_ROW_VALUE_OPTIONS_NONE, `${row.id}: PARTIAL_JSON rewrites an updated JSON column into a partial event`);
    // The commit-order proof the fence accepts on this engine (src/fence.rs), and the only way to
    // read the binlog while it is still being written. Both other settings are its preconditions.
    assert.equal(parameters[AURORA_ENHANCED_BINLOG_PARAMETER], BOOLEAN_ON, `${row.id}: without the enhanced binlog the fence has no proof and the log cannot be tailed`);
    assert.equal(parameters[AURORA_BINLOG_BACKUP_PARAMETER], BOOLEAN_OFF, `${row.id}: the enhanced binlog needs this off`);
    assert.equal(parameters[AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER], BOOLEAN_OFF, `${row.id}: the enhanced binlog needs this off`);
    assert.equal(parameters[REQUIRE_SECURE_TRANSPORT_PARAMETER], BOOLEAN_ON, `${row.id}: credentials never travel in the clear`);
    // Nothing else: no setting the suite depends on is left to whatever this release defaults to.
    assert.deepEqual(parameters, auroraClusterParametersFor(MatrixClusterEngine.AuroraMySQL), `${row.id}: the group is exactly the family's settings`);
  }
});

test('the Aurora MySQL groups ask for nothing Aurora cannot be asked for at create time', () => {
  const template = synth();
  for (const row of rowsFor(MatrixClusterEngine.AuroraMySQL)) {
    const group = groupOfCluster(template, clusterOfRow(template, row).properties);
    // GTIDs on Aurora are only *sometimes* modifiable — a staged transition and a reboot — so a
    // group cannot ask for them when the cluster is created; the row runs the no-GTID case, which
    // the enhanced binlog keeps exact. `src/engine.rs:117-119` withholds the GTID tip here too.
    assert.ok(!(GTID_MODE_PARAMETER in (group.Parameters as object)), `${row.id}: GTIDs cannot be asked for at create time on this engine`);
    assert.ok(!(ENFORCE_GTID_CONSISTENCY_PARAMETER in (group.Parameters as object)), row.id);
    // The fence reads the enhanced binlog's status instead; a variable that decides nothing on this
    // engine would only make the group look like the RDS one.
    assert.ok(!(BINLOG_ORDER_COMMITS_PARAMETER in (group.Parameters as object)), `${row.id}: the enhanced binlog is this engine's commit-order proof`);
    // Reader consistency stays at the engine default, which is what keeps a read after a write on
    // the promoted node from seeing the old leader's stale state.
    assert.ok(!(AURORA_REPLICA_READ_CONSISTENCY_PARAMETER in (group.Parameters as object)), `${row.id}: the default is the setting the failover row measures`);
    assert.ok(!(LOG_BIN_COMPRESS_PARAMETER in (group.Parameters as object)), row.id);
    for (const name of CLUSTER_FORBIDDEN_PARAMETERS[MatrixClusterEngine.AuroraMySQL]) {
      assert.ok(!(name in (group.Parameters as object)), `${row.id}: ${name} does not belong to this family`);
    }
  }
});

test('the Aurora PostgreSQL clusters decode logically, require TLS, and carry no MySQL name', () => {
  const template = synth();
  for (const row of rowsFor(MatrixClusterEngine.AuroraPostgreSQL)) {
    const group = groupOfCluster(template, clusterOfRow(template, row).properties);
    const parameters = group.Parameters as Record<string, string>;
    // The one switch that makes an Aurora PostgreSQL cluster a source at all.
    assert.equal(parameters[LOGICAL_REPLICATION_PARAMETER], BOOLEAN_ON, row.id);
    assert.equal(parameters[FORCE_SSL_PARAMETER], BOOLEAN_ON, row.id);
    for (const name of [MAX_WAL_SENDERS_PARAMETER, MAX_REPLICATION_SLOTS_PARAMETER]) {
      assert.equal(parameters[name], REPLICATION_CONNECTIONS, `${row.id}: ${name} must leave room for a reader`);
    }
    assert.deepEqual(parameters, auroraClusterParametersFor(MatrixClusterEngine.AuroraPostgreSQL), row.id);
    for (const name of CLUSTER_FORBIDDEN_PARAMETERS[MatrixClusterEngine.AuroraPostgreSQL]) {
      assert.ok(!(name in (parameters as object)), `${row.id}: ${name} is not a PostgreSQL setting`);
    }
  }
});

test('a failover row asks for the standby, and no other row pays for one', () => {
  const template = synth();
  const standalone = propertiesOf(template, 'AWS::RDS::DBInstance').filter((p) => p.DBClusterIdentifier === undefined);
  const withStandby = standalone.filter((p) => p.MultiAZ === true);
  assert.equal(withStandby.length, MULTI_AZ_TARGETS.length, 'only the rows the failover scenario runs on keep a standby');

  for (const row of MULTI_AZ_TARGETS) {
    const sameBuild = standalone.filter((p) => p.Engine === row.engine && p.EngineVersion === row.engineVersion && p.DBName === DEFAULT_DATABASE_NAME);
    // The gated build runs plain as well, so the standby is the only thing that tells the two rows
    // apart: exactly one instance of this build has one, and it is the failover row.
    const rowsOfBuild = [...RDS_TARGETS, ...MULTI_AZ_TARGETS].filter(
      (candidate) => candidate.engine === row.engine && candidate.engineVersion === row.engineVersion,
    );
    assert.equal(sameBuild.length, rowsOfBuild.length, `${row.id}: every row of this build is composed`);
    const failover = sameBuild.filter((p) => p.MultiAZ === true);
    assert.equal(failover.length, 1, `${row.id}: exactly one instance of this build keeps a standby`);
    assert.equal(failover[0]!.DBInstanceClass, row.instanceType, row.id);
    assert.equal(failover[0]!.PubliclyAccessible, false, row.id);
    for (const plain of sameBuild.filter((p) => p !== failover[0])) {
      // The plain twin is what the standby is compared against: same class, same private placement.
      assert.equal(plain.MultiAZ, false, `${row.id}: a standby nobody asked for doubles the bill`);
      assert.equal(plain.DBInstanceClass, row.instanceType, row.id);
      assert.equal(plain.PubliclyAccessible, false, row.id);
    }
  }

  for (const properties of standalone.filter((p) => p.MultiAZ !== true)) {
    assert.equal(properties.MultiAZ, false, 'a standby nobody asked for doubles the bill and proves nothing');
  }
});

test('nothing of a failover row survives the run, and every one of them scales into nothing', () => {
  const expiry = futureExpiry();
  const template = synth(undefined, expiry);

  const [subnetGroupId] = resourcesOf(template, 'AWS::RDS::DBSubnetGroup')[0]!;
  for (const row of AURORA_TARGETS) {
    const cluster = clusterOfRow(template, row);
    assert.equal(cluster.resource.DeletionPolicy, 'Delete', `${row.id}: a cluster left behind blocks the teardown and keeps billing`);
    assert.equal(cluster.resource.UpdateReplacePolicy, 'Delete', row.id);
    assert.equal(cluster.properties.DeletionProtection, false, `${row.id}: teardown by tag must never be blocked`);
    assert.equal(cluster.properties.StorageEncrypted, true, row.id);
    assert.ok(referencedId(cluster.properties.KmsKeyId), `${row.id}: the storage key is the run's own`);
    assert.equal(cluster.properties.AutoMinorVersionUpgrade, false, `${row.id}: a run tests the build its row names`);
    // No autoscaling, no serverless capacity: a run that would cost more has nowhere to grow.
    assert.equal(cluster.properties.ServerlessV2MinCapacity, undefined, row.id);
    assert.equal(cluster.properties.AuroraAutoPause, undefined, row.id);
    const retention = cluster.properties.BackupRetentionPeriod as number;
    assert.ok(
      typeof retention === 'number' && retention >= MIN_BACKUP_RETENTION_DAYS,
      `${row.id}: without automated backups Aurora keeps no binlogs and the binlog suite tests nothing`,
    );
    assert.equal(cluster.properties.MasterUsername, DEFAULT_ADMIN_USERNAME, row.id);
    assert.match(JSON.stringify(cluster.properties.MasterUserPassword), /resolve:secretsmanager:/, `${row.id}: the password is a reference, never a literal`);
    assert.deepEqual(
      tagValues(cluster.properties.Tags),
      { [EXPIRY_TAG_KEY]: expiry, [MANAGED_BY_TAG_KEY]: MANAGED_BY_TAG_VALUE, [RUN_ID_TAG_KEY]: RUN_ID },
      `${row.id}: an untagged cluster is invisible to cleanup`,
    );
    assert.deepEqual(
      cluster.properties.EnableCloudwatchLogsExports,
      [...exportedClusterLogTypes(row.engine)],
      `${row.id}: the engine logs the run collects, and no general query log`,
    );

    const group = groupOfCluster(template, cluster.properties);
    // The nodes start in the run's private subnets, the same group the instance rows use: nothing of
    // a run gets a second placement that a reviewer has to find.
    assert.equal(referencedId(cluster.properties.DBSubnetGroupName), subnetGroupId, `${row.id}: the cluster starts in the run's private subnet group`);
    const [groupId, groupResource] = resourcesOf(template, 'AWS::RDS::DBClusterParameterGroup').find(
      ([, r]) => (r.Properties as Record<string, unknown>).Family === group.Family,
    )!;
    assert.equal(groupResource.DeletionPolicy, 'Delete', `${groupId}: a group left behind blocks the next run`);
    assert.deepEqual(
      tagValues((groupResource.Properties as Record<string, unknown>).Tags),
      { [EXPIRY_TAG_KEY]: expiry, [MANAGED_BY_TAG_KEY]: MANAGED_BY_TAG_VALUE, [RUN_ID_TAG_KEY]: RUN_ID },
      groupId,
    );
  }

  // The log groups are the run's, capped, and destroyed with it: CDK only imports the names.
  const clusterLogs = propertiesOf(template, 'AWS::Logs::LogGroup').filter((p) => JSON.stringify(p.LogGroupName).includes('/cluster/'));
  assert.equal(clusterLogs.length, AURORA_TARGETS.reduce((total, row) => total + exportedClusterLogTypes(row.engine).length, 0), 'one group per exported log type of every cluster');
  for (const log of clusterLogs) {
    assert.equal(log.RetentionInDays, 7, 'a run keeps a week of engine logs and no more');
  }
});

test('a cluster asks for the settings of its own engine family and refuses a row it cannot fail over', () => {
  const stack = createMatrixStack(new App(), { targets: ['aurora-mysql-3'], expiry: futureExpiry(), runId: RUN_ID });
  const network = stack.node.findChild('Network') as unknown as MatrixNetwork;
  const secret = new MatrixAdminSecret(stack, 'RefusalSecret');
  const group = new MatrixClusterParameterGroup(stack, 'RefusalGroup', {
    engine: MatrixClusterEngine.AuroraMySQL,
    engineVersion: '8.0.mysql_aurora.3.12.0',
  });
  const base = {
    vpc: network.vpc,
    subnetGroup: network.databaseSubnetGroup,
    securityGroup: network.databaseSecurityGroup,
    engine: MatrixClusterEngine.AuroraMySQL,
    engineVersion: '8.0.mysql_aurora.3.12.0',
    instanceType: 'db.t4g.medium',
    secret,
    clusterParameterGroup: group,
  };

  // A cluster with no reader: the scenario would reboot the writer and call it a promotion.
  assert.throws(() => new MatrixAuroraCluster(stack, 'NoReaders', { ...base, readers: 0 }), /at least 1 reader in another availability zone/);
  // A group of another engine's family configures nothing this cluster reads.
  const wrongFamily = new MatrixClusterParameterGroup(stack, 'PostgresGroupOnMysql', {
    engine: MatrixClusterEngine.AuroraPostgreSQL,
    engineVersion: '16.13',
  });
  assert.throws(() => new MatrixAuroraCluster(stack, 'WrongFamily', { ...base, clusterParameterGroup: wrongFamily }), /configures something other than/);
  // A build that is not the one RDS spells would name a cluster the service does not offer.
  assert.throws(() => clusterEngineFor(MatrixClusterEngine.AuroraMySQL, '3.12.0'), /not the build RDS spells/);
  assert.throws(() => clusterEngineFor(MatrixClusterEngine.AuroraPostgreSQL, '16'), /not the build RDS spells/);
  // And a row cannot be failed over inside one zone.
  assert.throws(() => failoverZones(['us-east-1a'], 2, 'cluster node'), /at least 2 availability zones/);
  assert.throws(() => failoverZones(['us-east-1a', 'us-east-1a'], 2, 'cluster node'), /repeat themselves/);
  assert.throws(() => assertClusterParameterGroupFamilyFor(MatrixClusterEngine.AuroraMySQL, 'aurora-postgresql16'), /configures something other than/);
  assert.equal(assertClusterParameterGroupFamilyFor(MatrixClusterEngine.AuroraPostgreSQL, 'aurora-postgresql16'), 'aurora-postgresql16');
});

test('a run can name rows from both halves at once, and a name from neither is still an error', () => {
  const template = synth(['rds-pg-17-maz', 'aurora-mysql-84']);
  const standalone = propertiesOf(template, 'AWS::RDS::DBInstance').filter((p) => p.DBClusterIdentifier === undefined);
  assert.equal(standalone.length, 1, 'the Multi-AZ row is one instance');
  assert.equal(standalone[0]!.MultiAZ, true, 'and it is the failover variant of it');
  assert.equal(propertiesOf(template, 'AWS::RDS::DBCluster').length, 1, 'the Aurora row is one cluster');
  assert.equal(new Set(MATRIX_TARGETS.map((row: MatrixTarget) => row.id)).size, MATRIX_TARGETS.length, 'no two rows share a name');

  assert.throws(() => synth(['aurora-mysql-9']), /unknown target "aurora-mysql-9"/);
});
