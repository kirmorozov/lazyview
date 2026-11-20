/**
 * Engine settings of the weekly managed-database matrix.
 *
 * A managed database decides for itself how it writes its binlog, and the suite only measures what
 * the engine actually did — so the settings lazyview requires are not a preference here. A group
 * that left one out would let a target run green while testing a server lazyview would refuse to
 * start against. These groups are what make "tested on RDS" mean the same thing as "tested on the
 * image CI runs".
 *
 * Cost: a parameter group is a control-plane object, created once per engine family per run and
 * read by the service, never by a row. It adds nothing to the cost of a binlog event and nothing
 * per refresh; the group is free and it is DESTROYed with the run.
 *
 * Two settings are deliberately *not* here:
 *
 * * `log_bin` — RDS turns binary logging on with automated backups and does not expose the
 *   variable as a parameter, so the thing that keeps the binlog is the instance's backup retention
 *   (`lib/db-instance.ts` refuses a retention of zero for exactly this reason). Binlog *retention*
 *   hours is likewise not a parameter: it is `CALL mysql.rds_set_configuration('binlog retention
 *   hours', 24)` in the runbook's bootstrap — one administration call per run, on the admin
 *   account, at create time. It cannot live in a control store because it is an RDS
 *   server-administration procedure, not engine state the engine would read back.
 * * `binlog_row_image`, `gtid_mode`, `enforce_gtid_consistency` and `binlog_order_commits` on
 *   MariaDB — the MariaDB parameter families do not offer them (AWS lists them among the MySQL
 *   parameters unavailable for MariaDB), and MariaDB writes a full row image unconditionally, so
 *   there is nothing to turn on. A group that set one anyway would be refused when it is created,
 *   which is why the group for MariaDB asserts their *absence* instead (see `test/params.test.ts`).
 *   Running MariaDB without GTIDs is also the honest reading of the managed service: it is the
 *   no-GTID case, where lazyview takes the visibility-fence path rather than the GTID wait.
 */
import { RemovalPolicy } from 'aws-cdk-lib';
import * as rds from 'aws-cdk-lib/aws-rds';
import { Construct } from 'constructs';
import { assertParameterGroupFamilyFor, instanceEngineFor, MatrixEngine } from './db-instance';

/** The row-image and binlog-format settings every MySQL-family target must run with. */
export const BINLOG_FORMAT_PARAMETER = 'binlog_format';
export const BINLOG_ROW_IMAGE_PARAMETER = 'binlog_row_image';
export const BINLOG_ROW_VALUE_OPTIONS_PARAMETER = 'binlog_row_value_options';
/** The GTID pair, and the commit order the visibility fence depends on without GTIDs. */
export const GTID_MODE_PARAMETER = 'gtid_mode';
export const ENFORCE_GTID_CONSISTENCY_PARAMETER = 'enforce_gtid_consistency';
export const BINLOG_ORDER_COMMITS_PARAMETER = 'binlog_order_commits';
/** MariaDB's compressed binlog stream: lazyview refuses to read one. */
export const LOG_BIN_COMPRESS_PARAMETER = 'log_bin_compress';
/** TLS enforced, so a run on a managed database never sends credentials in the clear. */
export const REQUIRE_SECURE_TRANSPORT_PARAMETER = 'require_secure_transport';
/** The PostgreSQL settings logical decoding needs, and the slots to hold it. */
export const LOGICAL_REPLICATION_PARAMETER = 'rds.logical_replication';
export const FORCE_SSL_PARAMETER = 'rds.force_ssl';
export const MAX_WAL_SENDERS_PARAMETER = 'max_wal_senders';
export const MAX_REPLICATION_SLOTS_PARAMETER = 'max_replication_slots';

/** The values, spelled the way each parameter's family spells them. */
export const ROW_IMAGE_FULL = 'FULL';
export const BINLOG_FORMAT_ROW = 'ROW';
/**
 * Empty, not absent: `PARTIAL_JSON` rewrites an updated JSON column into a partial-update event
 * instead of the full row, and an absent parameter would only inherit whatever the engine default
 * happens to be today. The empty string is what states the requirement.
 */
export const BINLOG_ROW_VALUE_OPTIONS_NONE = '';
/** GTID and consistency are enumerated (ON / OFF / …); the rest are RDS booleans, which are 0/1. */
export const GTID_ENABLED = 'ON';
export const BOOLEAN_ON = '1';
export const BOOLEAN_OFF = '0';

/**
 * Replication connections a run needs: one suite run against one instance, with room for the
 * engine's own slots. A number this small costs nothing in shared memory and a slot that cannot be
 * created fails the run loudly instead of quietly reusing another test's slot.
 */
export const REPLICATION_CONNECTIONS = '10';

/** What each family must set, and what it must not even mention. Exported for the assertions. */
export const REQUIRED_PARAMETERS: Readonly<Record<MatrixEngine, readonly string[]>> = {
  [MatrixEngine.MySQL]: [
    BINLOG_FORMAT_PARAMETER,
    BINLOG_ROW_IMAGE_PARAMETER,
    BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
    GTID_MODE_PARAMETER,
    ENFORCE_GTID_CONSISTENCY_PARAMETER,
    BINLOG_ORDER_COMMITS_PARAMETER,
    REQUIRE_SECURE_TRANSPORT_PARAMETER,
  ],
  [MatrixEngine.MariaDB]: [BINLOG_FORMAT_PARAMETER, LOG_BIN_COMPRESS_PARAMETER, REQUIRE_SECURE_TRANSPORT_PARAMETER],
  [MatrixEngine.PostgreSQL]: [
    LOGICAL_REPLICATION_PARAMETER,
    FORCE_SSL_PARAMETER,
    MAX_WAL_SENDERS_PARAMETER,
    MAX_REPLICATION_SLOTS_PARAMETER,
  ],
};

/**
 * Names this family does not offer. They are not style preferences: RDS rejects the group when it
 * is created, so composing them into a group would turn the weekly run red at deploy time instead
 * of here.
 */
export const FORBIDDEN_PARAMETERS: Readonly<Record<MatrixEngine, readonly string[]>> = {
  // The MySQL-family names MariaDB has no equivalent of, and the two the other engines own.
  [MatrixEngine.MariaDB]: [
    BINLOG_ROW_IMAGE_PARAMETER,
    BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
    GTID_MODE_PARAMETER,
    ENFORCE_GTID_CONSISTENCY_PARAMETER,
    BINLOG_ORDER_COMMITS_PARAMETER,
    LOGICAL_REPLICATION_PARAMETER,
    FORCE_SSL_PARAMETER,
  ],
  [MatrixEngine.MySQL]: [LOG_BIN_COMPRESS_PARAMETER, LOGICAL_REPLICATION_PARAMETER, FORCE_SSL_PARAMETER],
  [MatrixEngine.PostgreSQL]: [
    BINLOG_FORMAT_PARAMETER,
    BINLOG_ROW_IMAGE_PARAMETER,
    LOG_BIN_COMPRESS_PARAMETER,
    GTID_MODE_PARAMETER,
    REQUIRE_SECURE_TRANSPORT_PARAMETER,
  ],
};

/**
 * The two engine families the matrix runs on Aurora, spelled the way RDS spells them. A cluster is
 * not an instance with a standby attached: it is a different resource, with its own parameter group
 * (a *cluster* parameter group) and its own parameter families, so it has its own engine names.
 */
export enum MatrixClusterEngine {
  AuroraMySQL = 'aurora-mysql',
  AuroraPostgreSQL = 'aurora-postgresql',
}

/**
 * The engine a client reaches an Aurora row with. A cluster answers on its family's port with its
 * family's client, so the runner's credential variable and the port rule that lets the task reach
 * it are derived from this, not from the cluster engine's name, which no client understands.
 */
export function clusterClientEngineFor(engine: MatrixClusterEngine): MatrixEngine {
  return engine === MatrixClusterEngine.AuroraMySQL ? MatrixEngine.MySQL : MatrixEngine.PostgreSQL;
}

/**
 * The Aurora switch that makes its binlog readable while it is being written. It is also what the
 * commit-order fence accepts in place of `binlog_order_commits`: `src/fence.rs:37-44` treats an
 * `aurora_enhanced_binlog` status of `ACTIVE` as the proof that a committed change is visible.
 */
export const AURORA_ENHANCED_BINLOG_PARAMETER = 'aurora_enhanced_binlog';
/** The two cluster settings the enhanced binlog has to be off before it will turn on. */
export const AURORA_BINLOG_BACKUP_PARAMETER = 'binlog_backup';
export const AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER = 'binlog_replication_globaldb';
/**
 * How stale a read from a reader may be. The matrix leaves it at the engine default (session-level,
 * so a read after a write on the promoted node cannot see the old leader's stale state), which is
 * why it must not appear in the group at all — see `CLUSTER_FORBIDDEN_PARAMETERS`.
 */
export const AURORA_REPLICA_READ_CONSISTENCY_PARAMETER = 'aurora_replica_read_consistency';
/**
 * The row image as the Aurora families spell it: the Aurora parameter reference lists `full`,
 * `minimal`, `noblob` in lower case, where the RDS MySQL families list `FULL`. The server reports
 * `FULL` either way, and lazyview reads the server's value, but a group that handed RDS the other
 * spelling would be refused when the run creates it.
 */
export const AURORA_ROW_IMAGE_FULL = 'full';

/** What each Aurora family must set in its cluster parameter group. Exported for the assertions. */
export const CLUSTER_REQUIRED_PARAMETERS: Readonly<Record<MatrixClusterEngine, readonly string[]>> = {
  [MatrixClusterEngine.AuroraMySQL]: [
    BINLOG_FORMAT_PARAMETER,
    BINLOG_ROW_IMAGE_PARAMETER,
    BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
    AURORA_ENHANCED_BINLOG_PARAMETER,
    AURORA_BINLOG_BACKUP_PARAMETER,
    AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER,
    REQUIRE_SECURE_TRANSPORT_PARAMETER,
  ],
  [MatrixClusterEngine.AuroraPostgreSQL]: [
    LOGICAL_REPLICATION_PARAMETER,
    FORCE_SSL_PARAMETER,
    MAX_WAL_SENDERS_PARAMETER,
    MAX_REPLICATION_SLOTS_PARAMETER,
  ],
};

/**
 * Names this Aurora family must not mention. Each of these would either be refused when the group is
 * created, or would silently replace the setting the row is there to measure.
 */
export const CLUSTER_FORBIDDEN_PARAMETERS: Readonly<Record<MatrixClusterEngine, readonly string[]>> = {
  [MatrixClusterEngine.AuroraMySQL]: [
    // GTIDs: AWS lists `gtid-mode` as only *sometimes* modifiable on Aurora — it moves through
    // OFF_PERMISSIVE and ON_PERMISSIVE and needs a reboot, so a group cannot ask for ON at create
    // time. The engine's own tip says the same thing from the other side: `src/engine.rs:117-119`
    // withholds the GTID advice on Aurora, "where the parameter does not exist as such". An Aurora
    // row therefore runs without GTIDs and is exact through the enhanced binlog instead, which is
    // the same no-GTID case the managed MariaDB rows run.
    GTID_MODE_PARAMETER,
    ENFORCE_GTID_CONSISTENCY_PARAMETER,
    // The fence reads the enhanced binlog's status here (`src/fence.rs:37-44`); a variable that
    // decides nothing on this engine would only make the group look like the RDS one.
    BINLOG_ORDER_COMMITS_PARAMETER,
    AURORA_REPLICA_READ_CONSISTENCY_PARAMETER,
    LOG_BIN_COMPRESS_PARAMETER,
    LOGICAL_REPLICATION_PARAMETER,
    FORCE_SSL_PARAMETER,
  ],
  [MatrixClusterEngine.AuroraPostgreSQL]: [
    BINLOG_FORMAT_PARAMETER,
    BINLOG_ROW_IMAGE_PARAMETER,
    BINLOG_ROW_VALUE_OPTIONS_PARAMETER,
    LOG_BIN_COMPRESS_PARAMETER,
    GTID_MODE_PARAMETER,
    ENFORCE_GTID_CONSISTENCY_PARAMETER,
    BINLOG_ORDER_COMMITS_PARAMETER,
    REQUIRE_SECURE_TRANSPORT_PARAMETER,
    AURORA_ENHANCED_BINLOG_PARAMETER,
    AURORA_BINLOG_BACKUP_PARAMETER,
    AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER,
    AURORA_REPLICA_READ_CONSISTENCY_PARAMETER,
  ],
};

/** The engine settings one Aurora family of the matrix runs with, in its cluster parameter group. */
export function auroraClusterParametersFor(engine: MatrixClusterEngine): Readonly<Record<string, string>> {
  if (engine === MatrixClusterEngine.AuroraMySQL) {
    return {
      // This one parameter is what makes an Aurora MySQL cluster a binlog source at all: RDS's
      // Aurora reference says `log-bin` follows `binlog_format` and is not settable on its own, and
      // the family's default is OFF. A cluster whose group leaves it out has no binlog to read.
      [BINLOG_FORMAT_PARAMETER]: BINLOG_FORMAT_ROW,
      [BINLOG_ROW_IMAGE_PARAMETER]: AURORA_ROW_IMAGE_FULL,
      // Empty, as on RDS: AWS's own prerequisites for reading an Aurora binlog say
      // `binlog_row_value_options` must not be `PARTIAL_JSON`, which rewrites an updated JSON
      // column into a partial event instead of the row a view was built from.
      [BINLOG_ROW_VALUE_OPTIONS_PARAMETER]: BINLOG_ROW_VALUE_OPTIONS_NONE,
      // The fence's proof on this engine, and the only way to read the binlog as it is written.
      [AURORA_ENHANCED_BINLOG_PARAMETER]: BOOLEAN_ON,
      // Both are preconditions of the enhanced binlog, not preferences: AWS requires them off
      // before it will turn on, and refuses the combination otherwise.
      [AURORA_BINLOG_BACKUP_PARAMETER]: BOOLEAN_OFF,
      [AURORA_BINLOG_REPLICATION_GLOBALDB_PARAMETER]: BOOLEAN_OFF,
      [REQUIRE_SECURE_TRANSPORT_PARAMETER]: BOOLEAN_ON,
    };
  }
  // Aurora PostgreSQL decodes logically with the same RDS switches as RDS PostgreSQL: the cluster
  // is a source only while `rds.logical_replication` is on, TLS is enforced by `rds.force_ssl`, and
  // the reader needs slots and wal senders to exist at all.
  return {
    [LOGICAL_REPLICATION_PARAMETER]: BOOLEAN_ON,
    [FORCE_SSL_PARAMETER]: BOOLEAN_ON,
    [MAX_WAL_SENDERS_PARAMETER]: REPLICATION_CONNECTIONS,
    [MAX_REPLICATION_SLOTS_PARAMETER]: REPLICATION_CONNECTIONS,
  };
}

/** The engine settings one family of the matrix runs with. */
export function matrixParametersFor(engine: MatrixEngine): Readonly<Record<string, string>> {
  if (engine === MatrixEngine.MySQL) {
    return {
      [BINLOG_FORMAT_PARAMETER]: BINLOG_FORMAT_ROW,
      [BINLOG_ROW_IMAGE_PARAMETER]: ROW_IMAGE_FULL,
      [BINLOG_ROW_VALUE_OPTIONS_PARAMETER]: BINLOG_ROW_VALUE_OPTIONS_NONE,
      [GTID_MODE_PARAMETER]: GTID_ENABLED,
      [ENFORCE_GTID_CONSISTENCY_PARAMETER]: GTID_ENABLED,
      [BINLOG_ORDER_COMMITS_PARAMETER]: BOOLEAN_ON,
      [REQUIRE_SECURE_TRANSPORT_PARAMETER]: BOOLEAN_ON,
    };
  }
  if (engine === MatrixEngine.MariaDB) {
    return {
      [BINLOG_FORMAT_PARAMETER]: BINLOG_FORMAT_ROW,
      [LOG_BIN_COMPRESS_PARAMETER]: BOOLEAN_OFF,
      [REQUIRE_SECURE_TRANSPORT_PARAMETER]: BOOLEAN_ON,
    };
  }
  return {
    [LOGICAL_REPLICATION_PARAMETER]: BOOLEAN_ON,
    [FORCE_SSL_PARAMETER]: BOOLEAN_ON,
    [MAX_WAL_SENDERS_PARAMETER]: REPLICATION_CONNECTIONS,
    [MAX_REPLICATION_SLOTS_PARAMETER]: REPLICATION_CONNECTIONS,
  };
}

export interface MatrixParameterGroupProps {
  readonly engine: MatrixEngine;
  /** The same full dotted build the instance runs, so the family is derived once, not twice. */
  readonly engineVersion: string;
  /** Extra settings for one row, merged over the family's set. @default none */
  readonly extraParameters?: Readonly<Record<string, string>>;
}

/**
 * The parameter group of one engine family. Every instance of that family in the run shares it:
 * one group per family, not per instance, because the settings are the family's and a run pays for
 * nothing by holding nine copies of them.
 */
export class MatrixParameterGroup extends Construct {
  /** The family the group configures, as RDS names it (`mysql8.4`, `mariadb10.11`, `postgres17`). */
  readonly family: string;
  /** The settings this group carries, for the run's own checks and for the assertions. */
  readonly parameters: Readonly<Record<string, string>>;
  readonly group: rds.IParameterGroup;

  constructor(scope: Construct, id: string, props: MatrixParameterGroupProps) {
    super(scope, id);
    // The engine object is the single source of both the build and its family name, so a group can
    // never disagree with the instance that uses it about which family it configures.
    const engine = instanceEngineFor(props.engine, props.engineVersion);
    const family = engine.parameterGroupFamily;
    if (!family) {
      throw new Error(`engine build "${props.engineVersion}" has no parameter group family`);
    }
    assertParameterGroupFamilyFor(props.engine, family);

    this.parameters = { ...matrixParametersFor(props.engine) };
    const required = REQUIRED_PARAMETERS[props.engine];
    const missing = required.filter((name) => this.parameters[name] === undefined);
    if (missing.length > 0) {
      throw new Error(`the ${family} group would leave ${missing.join(', ')} to the engine default`);
    }
    const forbidden = FORBIDDEN_PARAMETERS[props.engine].filter((name) => this.parameters[name] !== undefined);
    if (forbidden.length > 0) {
      throw new Error(`the ${family} family does not offer ${forbidden.join(', ')}: RDS would refuse the group`);
    }

    this.family = family;
    this.group = new rds.ParameterGroup(this, 'Group', {
      engine,
      description: `Engine settings of the ${family} targets of one managed-database test-matrix run`,
      parameters: { ...this.parameters },
      // Teardown is by tag, and a group left behind is a group the next run cannot create again.
      removalPolicy: RemovalPolicy.DESTROY,
    });
  }
}
