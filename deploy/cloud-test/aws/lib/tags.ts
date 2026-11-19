/**
 * Tagging rules for the weekly managed-database test matrix.
 *
 * Every resource a run creates carries the same three tags. `expiry` is what the cleanup job
 * deletes on when a run dies mid-flight, `run-id` ties a resource and its bill back to one
 * workflow run, and `managed-by` is the key the owner's budget alarm is set on. They are the only
 * tags this composition sets, and they name nothing beyond the run itself.
 */
import { Aspects, IAspect, Resource, Tags } from 'aws-cdk-lib';
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

/** Puts the three matrix tags on every taggable resource in the tree it is applied to. */
export class MatrixTags implements IAspect {
  private readonly entries: ReadonlyArray<readonly [string, string]>;

  constructor(props: MatrixTagProps) {
    this.entries = [
      [EXPIRY_TAG_KEY, assertValidExpiry(props.expiry)],
      [RUN_ID_TAG_KEY, props.runId],
      [MANAGED_BY_TAG_KEY, MANAGED_BY_TAG_VALUE],
    ];
  }

  visit(node: IConstruct): void {
    // A Resource is the only node that renders a tag list CloudFormation reads; constructs that
    // only group or configure others have nothing to tag.
    if (!(node instanceof Resource)) {
      return;
    }
    const tags = Tags.of(node);
    for (const [key, value] of this.entries) {
      tags.add(key, value);
    }
  }
}

/**
 * Tags everything the run creates. A crashed run stays findable, stays billable to one alarm, and
 * stays deletable by the cleanup job, which is what makes a weekly cloud job safe to run at all.
 */
export function applyMatrixTags(scope: IConstruct, props: MatrixTagProps): void {
  Aspects.of(scope).add(new MatrixTags(props));
}
