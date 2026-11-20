/**
 * The failover variants of the weekly managed-database matrix.
 *
 * A failover run promotes something. On RDS that something is a standby the service keeps in a
 * second availability zone and swaps in with `reboot-db-instance --force-failover`; on Aurora it is
 * a reader the cluster promotes with `failover-db-cluster`. Either way the scenario is the one the
 * GTID plan already owns (`tests/failover_source.rs`: swap the DSN to the promoted endpoint and
 * re-assert "resume without rebuild"), so this file adds no test logic — it adds the *shape* the
 * scenario needs, and the rules that make that shape real:
 *
 * * A failover row always has a second node. A row that asks to be failed over with nothing to
 *   promote promotes nothing, and the run would go green having measured a reboot.
 * * The second node is somewhere else. A standby in the writer's own availability zone survives the
 *   scenario that exercises it and nothing else, so the zones a row spreads over have to be distinct.
 * * Only the rows the scenario runs on pay for one. A standby doubles what an instance costs for the
 *   length of the run, so it is a property of a row, never a default: the single-AZ rows that make
 *   up most of the matrix stay single-AZ.
 *
 * Cost: one extra instance of the same class per failover row per run (about the price of the first
 * one, so a Multi-AZ row costs roughly twice its single-AZ twin). Nothing here runs per binlog
 * event — the standby is a resource the service keeps, not code the run calls.
 */
import { Construct } from 'constructs';
import { MatrixDbInstance, type MatrixDbInstanceProps } from './db-instance';
import { MIN_MATRIX_AZS } from './network';

/** The fewest second nodes a failover row may have: the one the scenario promotes. */
export const MIN_STANDBY_INSTANCES = 1;

/**
 * Refuses a failover row with nothing to promote, and returns the count that passed. Named for what
 * it stops: a run that reports "failover tested" after rebooting the only node it had.
 */
export function requireStandby(kind: string, count: number): number {
  if (!Number.isInteger(count) || count < MIN_STANDBY_INSTANCES) {
    throw new Error(
      `a failover row needs at least ${MIN_STANDBY_INSTANCES} ${kind}: with nothing to promote, the scenario reboots the only node and measures that`,
    );
  }
  return count;
}

/**
 * The availability zones a row's nodes are spread over, and the check that they really are distinct:
 * a standby in the writer's own zone is a copy, not a failover. Takes the zones it is given rather
 * than looking them up, so the same rule holds whether the run resolved the region or not.
 */
export function failoverZones(zones: readonly string[], nodes: number, kind: string): string[] {
  if (zones.length < MIN_MATRIX_AZS) {
    throw new Error(`a failover row needs at least ${MIN_MATRIX_AZS} availability zones to spread ${nodes} ${kind}(s) over, got ${zones.length}`);
  }
  if (new Set(zones).size !== zones.length) {
    throw new Error(`the availability zones of a failover row repeat themselves (${zones.join(', ')}): a standby in the writer's own zone is not a failover`);
  }
  const picked = zones.slice(0, nodes);
  if (picked.length < nodes) {
    throw new Error(`${nodes} ${kind}(s) need ${nodes} availability zones and the run has ${zones.length}: two nodes in one zone is a copy, not a failover`);
  }
  return picked;
}

export type MatrixMultiAzInstanceProps = Omit<MatrixDbInstanceProps, 'multiAz'>;

/**
 * An RDS row with the standby the failover scenario promotes. The instance itself is the ordinary
 * one — same private subnets, same parameter group, same secret — with the standby asked for, so
 * what a failover row measures and what the plain row measures cannot drift apart.
 */
export class MatrixMultiAzInstance extends Construct {
  readonly db: MatrixDbInstance;
  /** What the run is told the standby is for, kept so the reason travels with the resource. */
  readonly reason: string;

  constructor(scope: Construct, id: string, props: MatrixMultiAzInstanceProps, reason: string = MULTI_AZ_REASON) {
    super(scope, id);
    this.reason = reason;
    this.db = new MatrixDbInstance(this, 'Instance', { ...props, multiAz: true });
  }
}

/** Why a Multi-AZ row exists, in the words the run and the reader both see. */
export const MULTI_AZ_REASON =
  'the failover row: RDS keeps this standby in another availability zone and swaps it in with reboot-db-instance --force-failover';
