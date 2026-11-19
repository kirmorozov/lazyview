/**
 * The master credential of a run's databases, held in Secrets Manager.
 *
 * No password is ever a template literal or a parameter: the secret is created with a generated
 * password under a customer-managed KMS key, and the databases read it through a dynamic
 * reference. These are ordinary database accounts, not privileged ones — the point of the matrix
 * (M-03) is to test against exactly the privileges a managed service hands out, and no RDS or
 * Aurora account has SUPER. The unprivileged accounts the suites connect with are created by the
 * run's bootstrap SQL, which mirrors what `docker/initdb` grants locally.
 */
import { RemovalPolicy } from 'aws-cdk-lib';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

/** Master account of a run's databases. Neutral by design: it identifies the run, not a product. */
export const DEFAULT_ADMIN_USERNAME = 'matrix_admin';

/** Long enough that no brute force matters for a resource that lives for hours. */
export const ADMIN_PASSWORD_LENGTH = 32;

/** Characters that break a password the moment it is quoted into a DSN or a shell. */
export const EXCLUDED_PASSWORD_CHARACTERS = '"@/\\';

export interface MatrixAdminSecretProps {
  /** @default DEFAULT_ADMIN_USERNAME */
  readonly username?: string;
  /** Key the secret is stored under. @default a new, rotation-enabled key owned by the run */
  readonly encryptionKey?: kms.IKey;
}

export class MatrixAdminSecret extends Construct {
  readonly secret: secretsmanager.ISecret;
  readonly encryptionKey: kms.IKey;
  readonly username: string;

  constructor(scope: Construct, id: string, props: MatrixAdminSecretProps = {}) {
    super(scope, id);
    this.username = props.username ?? DEFAULT_ADMIN_USERNAME;
    this.encryptionKey =
      props.encryptionKey ??
      new kms.Key(this, 'SecretKey', {
        description: 'Credentials of one managed-database test run',
        enableKeyRotation: true,
        removalPolicy: RemovalPolicy.DESTROY,
      });

    this.secret = new secretsmanager.Secret(this, 'Admin', {
      description: 'Master credential of the managed databases of one test-matrix run',
      encryptionKey: this.encryptionKey,
      removalPolicy: RemovalPolicy.DESTROY,
      generateSecretString: {
        passwordLength: ADMIN_PASSWORD_LENGTH,
        excludeCharacters: EXCLUDED_PASSWORD_CHARACTERS,
        includeSpace: false,
        secretStringTemplate: JSON.stringify({ username: this.username }),
        generateStringKey: 'password',
      },
    });
  }
}
