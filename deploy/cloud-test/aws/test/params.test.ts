/**
 * Offline proof of the engine settings a managed target has to run with. No account, no
 * credentials, no network: the assertions read the template `cdk synth` would hand CloudFormation.
 *
 * They are the acceptance criteria for this half of the AWS lane, and they read the template rather
 * than the library's own model on purpose: what the service is *asked* for is what the weekly run
 * measures, and a setting that never reaches the template measures nothing. Each group is looked up
 * through the reference its instance carries, so a group that exists but is attached to nothing
 * fails here.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';

import { App } from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';

import { createMatrixStack, RDS_TARGETS, type MatrixTarget } from '../bin/app';
import { assertParameterGroupFamilyFor, MatrixEngine } from '../lib/db-instance';
import {
  BINLOG_FORMAT_PARAMETER,
  BINLOG_ORDER_COMMITS_PARAMETER,
  BINLOG_ROW_IMAGE_PARAMETER,
  BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
  BINLOG_ROW_VALUE_OPTIONS_NONE,
  BOOLEAN_OFF,
  BOOLEAN_ON,
  ENFORCE_GTID_CONSISTENCY_PARAMETER,
  FORCE_SSL_PARAMETER,
  GTID_ENABLED,
  GTID_MODE_PARAMETER,
  LOG_BIN_COMPRESS_PARAMETER,
  LOGICAL_REPLICATION_PARAMETER,
  MAX_REPLICATION_SLOTS_PARAMETER,
  MAX_WAL_SENDERS_PARAMETER,
  MatrixParameterGroup,
  matrixParametersFor,
  REPLICATION_CONNECTIONS,
  REQUIRE_SECURE_TRANSPORT_PARAMETER,
  ROW_IMAGE_FULL,
} from '../lib/params';
import { EXPIRY_TAG_KEY, MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE, RUN_ID_TAG_KEY } from '../lib/tags';

const RUN_ID = 'test-run';

/** A run with time left on its clock, so nothing has a reason to refuse it. */
function futureExpiry(): string {
  return new Date(Date.now() + 60 * 60 * 1000).toISOString();
}

/**
 * A2's proof is about the instance rows and the group each one reads, so a default synth means those:
 * a failover row is the same build twice, and the Aurora rows are composed by `lib/aurora-cluster.ts`.
 * `test/failover.test.ts` measures the failover rows and the clusters.
 */
function synth(targets?: readonly string[], expiry: string = futureExpiry()): Template {
  return Template.fromStack(createMatrixStack(new App(), { targets: targets ?? RDS_TARGETS.map((target) => target.id), expiry, runId: RUN_ID }));
}

function propertiesOf(template: Template, type: string): Array<Record<string, unknown>> {
  return Object.values(template.findResources(type)).map((r) => (r as { Properties: Record<string, unknown> }).Properties);
}

/** The logical id a `{ Ref: … }` points at, or nothing when the value is not a reference. */
function referencedId(value: unknown): string | undefined {
  const ref = (value as { Ref?: unknown })?.Ref;
  return typeof ref === 'string' ? ref : undefined;
}

/**
 * The parameter group an instance is configured with, found by following the reference the instance
 * carries. A name written as a literal string, or a reference to something that is not a parameter
 * group of this run, is a group the database would never read.
 */
function groupOfInstance(template: Template, instance: Record<string, unknown>): Record<string, unknown> {
  const id = referencedId(instance.DBParameterGroupName);
  assert.ok(id, "the instance names its parameter group by reference, so it can only be this run's");
  const group = (template.findResources('AWS::RDS::DBParameterGroup') as Record<string, unknown>)[id];
  assert.ok(group, `the parameter group "${id}" the instance refers to exists in this run`);
  return (group as { Properties: Record<string, unknown> }).Properties;
}

function instancesOf(template: Template, target: MatrixTarget): Array<Record<string, unknown>> {
  return propertiesOf(template, 'AWS::RDS::DBInstance').filter(
    (p) => p.Engine === target.engine && p.EngineVersion === target.engineVersion,
  );
}

/** The one instance of a target row, and the settings it is configured with. */
function parametersOf(template: Template, target: MatrixTarget): Record<string, string> {
  const instances = instancesOf(template, target);
  assert.equal(instances.length, 1, `the matrix has exactly one row named ${target.id}`);
  return groupOfInstance(template, instances[0]!).Parameters as Record<string, string>;
}

function targetsFor(engine: MatrixEngine): MatrixTarget[] {
  return RDS_TARGETS.filter((target) => target.engine === engine);
}

/** The three tags a run's resources carry, read straight out of the template. */
function tagValues(tags: unknown): Record<string, string> {
  const list = tags as Array<{ Key: string; Value: string }>;
  assert.ok(Array.isArray(list), 'the resource renders a tag list');
  return Object.fromEntries(list.map((tag) => [tag.Key, tag.Value]));
}

/** The family RDS names for a pinned build: the engine plus the major it matches a family on. */
function familyOf(target: MatrixTarget): string {
  const [major, minor] = target.engineVersion.split('.');
  return target.engine === MatrixEngine.PostgreSQL ? `${target.engine}${major}` : `${target.engine}${major}.${minor}`;
}

test('every target is configured with the parameter group of its own engine family', () => {
  const template = synth();
  // One group per family, not per instance: the settings belong to the family, and a run that paid
  // for a copy per instance would buy nothing with it.
  template.resourceCountIs('AWS::RDS::DBParameterGroup', new Set(RDS_TARGETS.map(familyOf)).size);

  for (const target of RDS_TARGETS) {
    const instances = instancesOf(template, target);
    assert.equal(instances.length, 1, `the matrix has exactly one row named ${target.id}`);
    const family = groupOfInstance(template, instances[0]!).Family as string;
    assert.equal(family, familyOf(target), `${target.id}: the group configures the wrong family`);
  }
});

test('the MySQL groups run the binlog lazyview will start against', () => {
  const template = synth();
  for (const target of targetsFor(MatrixEngine.MySQL)) {
    const parameters = parametersOf(template, target);
    // The settings lazyview refuses to start without, each asserted by name and value.
    assert.equal(parameters[BINLOG_FORMAT_PARAMETER], 'ROW', target.id);
    assert.equal(parameters[BINLOG_ROW_IMAGE_PARAMETER], ROW_IMAGE_FULL, target.id);
    assert.equal(
      parameters[BINLOG_ROW_VALUE_OPTIONS_PARAMETER],
      BINLOG_ROW_VALUE_OPTIONS_NONE,
      `${target.id}: a partial JSON update event is not the row a view was built from`,
    );
    // And nothing else: every name the family offers and the run needs is set, so no setting a
    // suite depends on is left to whatever the service defaults to this quarter.
    assert.deepEqual(parameters, matrixParametersFor(MatrixEngine.MySQL), `${target.id}: the group is exactly the family's settings`);
  }
});

test('the MySQL groups run with GTIDs and commit order, so a resume needs no rebuild', () => {
  const template = synth();
  for (const target of targetsFor(MatrixEngine.MySQL)) {
    const parameters = parametersOf(template, target);
    assert.equal(parameters[GTID_MODE_PARAMETER], GTID_ENABLED, target.id);
    assert.equal(parameters[ENFORCE_GTID_CONSISTENCY_PARAMETER], GTID_ENABLED, target.id);
    // Without GTIDs this is what makes the visibility fence exact; without it lazyview only warns.
    assert.equal(parameters[BINLOG_ORDER_COMMITS_PARAMETER], BOOLEAN_ON, target.id);
  }
});

test('the MariaDB groups set ROW, refuse a compressed binlog, and carry no name RDS would reject', () => {
  const template = synth();
  for (const target of targetsFor(MatrixEngine.MariaDB)) {
    const group = groupOfInstance(template, instancesOf(template, target)[0]!);
    const family = group.Family as string;
    assert.ok(family.startsWith('mariadb'), `${target.id}: ${family} is not a MariaDB family`);
    template.hasResourceProperties('AWS::RDS::DBParameterGroup', {
      Family: family,
      Parameters: {
        [BINLOG_FORMAT_PARAMETER]: 'ROW',
        // A compressed binlog is one lazyview will not read at all, so it is off, not unset.
        [LOG_BIN_COMPRESS_PARAMETER]: BOOLEAN_OFF,
        [REQUIRE_SECURE_TRANSPORT_PARAMETER]: BOOLEAN_ON,
        // The MariaDB families do not offer these, and naming one would make RDS refuse the group
        // when the run creates it. MariaDB writes a full row image unconditionally, and it runs
        // here without GTIDs: that is the no-GTID case, where lazyview takes the fence path.
        [BINLOG_ROW_IMAGE_PARAMETER]: Match.absent(),
        [BINLOG_ROW_VALUE_OPTIONS_PARAMETER]: Match.absent(),
        [GTID_MODE_PARAMETER]: Match.absent(),
        [ENFORCE_GTID_CONSISTENCY_PARAMETER]: Match.absent(),
        [BINLOG_ORDER_COMMITS_PARAMETER]: Match.absent(),
      },
    });
    assert.deepEqual(group.Parameters, matrixParametersFor(MatrixEngine.MariaDB), target.id);
  }
});

test('the PostgreSQL groups decode logically and require TLS, with slots for the reader', () => {
  const template = synth();
  for (const target of targetsFor(MatrixEngine.PostgreSQL)) {
    const parameters = parametersOf(template, target);
    // The one switch that makes a managed PostgreSQL a binlog source at all.
    assert.equal(parameters[LOGICAL_REPLICATION_PARAMETER], BOOLEAN_ON, target.id);
    assert.equal(parameters[FORCE_SSL_PARAMETER], BOOLEAN_ON, target.id);
    for (const name of [MAX_WAL_SENDERS_PARAMETER, MAX_REPLICATION_SLOTS_PARAMETER]) {
      assert.equal(parameters[name], REPLICATION_CONNECTIONS, `${target.id}: ${name} must leave room for a reader`);
    }
    assert.deepEqual(parameters, matrixParametersFor(MatrixEngine.PostgreSQL), target.id);
  }
});

test('every group keeps credentials off the wire and is destroyed with the run', () => {
  const expiry = futureExpiry();
  const template = synth(undefined, expiry);
  const groups = template.findResources('AWS::RDS::DBParameterGroup');
  for (const [id, resource] of Object.entries(groups)) {
    const res = resource as { Properties: Record<string, unknown>; DeletionPolicy?: string; UpdateReplacePolicy?: string };
    const parameters = res.Properties.Parameters as Record<string, string>;
    const family = res.Properties.Family as string;
    if (!family.startsWith('postgres')) {
      // MySQL and MariaDB have a parameter for this; PostgreSQL's is rds.force_ssl, asserted above.
      assert.equal(parameters[REQUIRE_SECURE_TRANSPORT_PARAMETER], BOOLEAN_ON, `${id}: ${family} may be spoken to in the clear`);
    }
    // Nothing of a run is left behind for the next one, or for the bill, to rediscover.
    assert.equal(res.DeletionPolicy, 'Delete', `${id}: a group left behind blocks the next run`);
    assert.equal(res.UpdateReplacePolicy, 'Delete', id);
    assert.deepEqual(
      tagValues(res.Properties.Tags),
      { [EXPIRY_TAG_KEY]: expiry, [MANAGED_BY_TAG_KEY]: MANAGED_BY_TAG_VALUE, [RUN_ID_TAG_KEY]: RUN_ID },
      `${id}: an untagged group is invisible to cleanup`,
    );
  }
  assert.ok(Object.keys(groups).length > 0, 'the run configures at least one family');
});

test('binlogs exist only while backups do, so every instance keeps a retention', () => {
  const template = synth();
  for (const properties of propertiesOf(template, 'AWS::RDS::DBInstance')) {
    const retention = properties.BackupRetentionPeriod as number;
    assert.ok(
      typeof retention === 'number' && retention >= 1,
      'RDS keeps no binlogs without automated backups, so an instance with none tests nothing',
    );
  }
});

test('a group is refused for a build that is not a full pinned version, and a family is refused for the wrong engine', () => {
  const stack = createMatrixStack(new App(), { targets: ['rds-mysql-84'], expiry: futureExpiry(), runId: RUN_ID });
  // A floating major would name a family the run never pinned, so the build has to be complete.
  assert.throws(
    () => new MatrixParameterGroup(stack, 'FloatingBuild', { engine: MatrixEngine.MySQL, engineVersion: '8' }),
    /not a full dotted build/,
  );
  // The rule the instance checks before it accepts a group: a group of another engine's family sets
  // binlog parameters nothing reads, and the run would measure an unconfigured server.
  assert.throws(() => assertParameterGroupFamilyFor(MatrixEngine.MySQL, 'mariadb10.11'), /configures something other than/);
  assert.throws(() => assertParameterGroupFamilyFor(MatrixEngine.PostgreSQL, 'mysql8.4'), /configures something other than/);
  assert.equal(assertParameterGroupFamilyFor(MatrixEngine.MariaDB, 'mariadb11.4'), 'mariadb11.4');
});
