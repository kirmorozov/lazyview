/**
 * The one bucket a test run writes to and reads from: the test binaries the CI runner built go in,
 * the artifacts the in-VPC task collects come out.
 *
 * Why the binaries go through a bucket instead of being built in the task: the task has no internet
 * path (that is the point of the composition in `runner.ts`), so it cannot fetch a toolchain, and
 * building in-region would throw away the rust cache CI already has. The runner writes the objects
 * from outside; the task reads them from inside the VPC and nothing else can — `denyReadsOutsideVpc`
 * turns that into a policy statement rather than a habit. Writes stay open to the orchestrator and
 * to the cleanup job on purpose: a bucket that cannot be emptied cannot be destroyed, and a bucket
 * that cannot be written to from outside cannot be seeded at all.
 *
 * Cost: nothing here runs per binlog event — this composition touches no engine file and no hot
 * path. Recurring cost is storage alone (a run's binaries and logs are tens of megabytes: cents a
 * month, and the objects are gone in days) plus the SSE-KMS request charge on each put and get,
 * which at a few hundred requests per run is rounding error. Every object expires on a lifecycle
 * rule and the bucket itself is DESTROYed, so a run that dies mid-flight leaves nothing billing.
 */
import { Duration, RemovalPolicy, Token } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

/** Where a run's prebuilt test binaries live. The task may read here and nowhere else. */
export const BINARIES_PREFIX = 'binaries';

/** Where a run's collected artifacts live. The task may write here and nowhere else. */
export const ARTIFACTS_PREFIX = 'artifacts';

/** Longest an object may sit in the bucket. A run is hours; the extra day covers the report upload. */
export const MAX_OBJECT_LIFETIME = Duration.days(3);

/** An upload that stalled is not a reason to pay for parts forever. */
export const ABORT_MULTIPART_AFTER = Duration.days(1);

export interface MatrixArtifactBucketProps {
  /** @default MAX_OBJECT_LIFETIME, and never more than that */
  readonly objectLifetime?: Duration;
  /** Key the objects are encrypted with. @default a new key the run owns and destroys */
  readonly encryptionKey?: kms.IKey;
}

export class MatrixArtifactBucket extends Construct {
  readonly bucket: s3.Bucket;
  readonly encryptionKey: kms.IKey;

  constructor(scope: Construct, id: string, props: MatrixArtifactBucketProps = {}) {
    super(scope, id);
    const objectLifetime = props.objectLifetime ?? MAX_OBJECT_LIFETIME;
    const seconds = objectLifetime.toSeconds();
    if (seconds < 1 || seconds > MAX_OBJECT_LIFETIME.toSeconds()) {
      throw new Error(`object lifetime must be between 1 second and ${MAX_OBJECT_LIFETIME.toSeconds()} seconds, got ${seconds}`);
    }

    this.encryptionKey =
      props.encryptionKey ??
      new kms.Key(this, 'BucketKey', {
        description: 'Artifacts of one managed-database test run',
        enableKeyRotation: true,
        removalPolicy: RemovalPolicy.DESTROY,
      });

    this.bucket = new s3.Bucket(this, 'Bucket', {
      // No name: nothing looks the bucket up by name (the task is handed it by reference), and a
      // run that fixed its own name could not deploy two runs at once.
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: this.encryptionKey,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      // The suites are TLS-only anyway, and a plain-HTTP GET of a run's binaries is the one path
      // that would hand a build to whoever is watching the network.
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      // Versioning off: one build per path, no history worth keeping, and every kept version is an
      // object that survives the delete of its own key and bills past the run.
      removalPolicy: RemovalPolicy.DESTROY,
      // Teardown is by tag and must never be blocked, so the bucket empties itself on delete: the
      // lifecycle sweep is daily and a run is hours, so without this one late artifact would strand
      // the bucket. It costs one custom-resource call per run, at teardown, and nothing else.
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          expiration: objectLifetime,
          abortIncompleteMultipartUploadAfter: ABORT_MULTIPART_AFTER,
        },
      ],
    });
  }

  /**
   * Reads are served only from inside the run's VPC; writes and deletes stay open to whoever has
   * IAM permission. That is what lets the runner seed the bucket from outside while the task — the
   * only thing that reads it — is inside, and it leaves the cleanup job able to delete what the
   * lifecycle rule has not swept yet.
   */
  denyReadsOutsideVpc(endpointId: string): void {
    if (!Token.isUnresolved(endpointId) && !/^vpce-[0-9a-z]+$/.test(endpointId)) {
      throw new Error(`"${endpointId}" is not a VPC endpoint id (vpce-...)`);
    }
    this.bucket.addToResourcePolicy(
      new iam.PolicyStatement({
        sid: 'ReadsOnlyFromInsideTheRun',
        effect: iam.Effect.DENY,
        principals: [new iam.AnyPrincipal()],
        actions: ['s3:GetObject'],
        resources: [`${this.bucket.bucketArn}/*`],
        conditions: { StringNotEquals: { 'aws:SourceVpce': [endpointId] } },
      }),
    );
  }

  /** Lets `grantee` fetch the prebuilt binaries, and nothing else in the bucket. */
  grantReadBinaries(grantee: iam.IGrantable): void {
    this.bucket.grantRead(grantee, `${BINARIES_PREFIX}/*`);
  }

  /** Lets `grantee` upload collected artifacts, and nothing else in the bucket. */
  grantWriteArtifacts(grantee: iam.IGrantable): void {
    this.bucket.grantPut(grantee, `${ARTIFACTS_PREFIX}/*`);
  }
}
