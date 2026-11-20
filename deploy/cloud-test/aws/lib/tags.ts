/**
 * Tagging rules for the weekly managed-database test matrix.
 *
 * Every resource a run creates carries the same three tags. `expiry` is what the cleanup job
 * deletes on when a run dies mid-flight, `run-id` ties a resource and its bill back to one
 * workflow run, and `managed-by` is the key the owner's budget alarm is set on. They are the only
 * tags this composition sets, and they name nothing beyond the run itself.
 *
 * They are applied with CDK's own tag aspect, added once to the stack: asking for a tag is itself
 * an aspect, so asking for one from *inside* another aspect's visit makes synthesis restart its
 * aspect passes for every resource in the run, and past a hundred resources it gives up. The
 * standard mechanism adds the aspect once, before synthesis starts, and reaches everything in the
 * tree that can carry a tag — which is the whole point of tagging a run rather than a resource.
 */
import { Tags } from 'aws-cdk-lib';
import type { IConstruct } from 'constructs';

export const EXPIRY_TAG_KEY = 'expiry';
export const RUN_ID_TAG_KEY = 'run-id';
export const MANAGED_BY_TAG_KEY = 'managed-by';
export const MANAGED_BY_TAG_VALUE = 'cloud-db-matrix';

/** Nothing a run creates is allowed to outlive the run by more than this. */
export const DEFAULT_MAX_LIFE_HOURS = 4;

export interface MatrixTagProps {
  /** ISO-8601 UTC instant after which every resource of the run may be deleted. */
  readonly expiry: string;
  /** Ties the run's resources, and the bill for them, to one workflow run. */
  readonly runId: string;
}

/** The latest instant a run that starts now may leave resources behind. */
export function expiryFromNow(maxLifeHours: number = DEFAULT_MAX_LIFE_HOURS): string {
  if (!Number.isFinite(maxLifeHours) || maxLifeHours <= 0) {
    throw new Error(`max life must be a positive number of hours, got ${maxLifeHours}`);
  }
  return new Date(Date.now() + maxLifeHours * 60 * 60 * 1000).toISOString();
}

/**
 * Rejects an expiry that could not schedule a cleanup, and returns the ones that can. A run that
 * cannot outlive its own deploy has no reason to create anything.
 */
export function assertValidExpiry(expiry: string): string {
  const at = Date.parse(expiry);
  if (Number.isNaN(at)) {
    throw new Error(`expiry "${expiry}" is not an ISO-8601 instant`);
  }
  if (at <= Date.now()) {
    throw new Error(`expiry "${expiry}" is already past: nothing is deployed against a dead run`);
  }
  return expiry;
}

/** The three tags a run's resources carry, in the order the reader lists them. */
export function matrixTagEntries(props: MatrixTagProps): ReadonlyArray<readonly [string, string]> {
  return [
    [EXPIRY_TAG_KEY, assertValidExpiry(props.expiry)],
    [RUN_ID_TAG_KEY, props.runId],
    [MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE],
  ];
}

/**
 * Tags everything the run creates. A crashed run stays findable, stays billable to one alarm, and
 * stays deletable by the cleanup job, which is what makes a weekly cloud job safe to run at all.
 * An expiry that has already passed refuses the whole run rather than tagging a dead one.
 */
export function applyMatrixTags(scope: IConstruct, props: MatrixTagProps): void {
  const tags = Tags.of(scope);
  for (const [key, value] of matrixTagEntries(props)) {
    tags.add(key, value);
  }
}
