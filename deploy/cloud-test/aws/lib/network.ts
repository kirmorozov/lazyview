/**
 * The network one managed-database test run lives in.
 *
 * The layout is the cheapest one that is still the real thing: two isolated subnets, no internet
 * gateway, no NAT gateway, no default route anywhere. A database in here cannot get a public
 * endpoint, and the run's compute reaches it from inside the VPC (the test task itself is PR A4).
 * That is also what makes the least-privilege claim this matrix exists for testable offline: a run
 * cannot leave a managed database reachable from outside its own VPC, because there is no path
 * from outside to anything.
 */
import { RemovalPolicy } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as rds from 'aws-cdk-lib/aws-rds';
import { Construct } from 'constructs';

/** Private space for one run; /20 leaves a subnet pair per AZ plus room for the test task. */
export const MATRIX_VPC_CIDR = '10.70.0.0/20';

/** A DB subnet group needs two AZs, and a failover scenario needs a second AZ to promote into. */
export const MIN_MATRIX_AZS = 2;

export interface MatrixNetworkProps {
  /** @default MATRIX_VPC_CIDR */
  readonly cidr?: string;
  /** @default MIN_MATRIX_AZS */
  readonly maxAzs?: number;
}

export class MatrixNetwork extends Construct {
  readonly vpc: ec2.IVpc;
  readonly databaseSecurityGroup: ec2.ISecurityGroup;
  readonly databaseSubnetGroup: rds.SubnetGroup;

  constructor(scope: Construct, id: string, props: MatrixNetworkProps = {}) {
    super(scope, id);
    const maxAzs = props.maxAzs ?? MIN_MATRIX_AZS;
    if (maxAzs < MIN_MATRIX_AZS) {
      throw new Error(`a run needs at least ${MIN_MATRIX_AZS} availability zones (subnet group, failover target), got ${maxAzs}`);
    }

    const vpc = new ec2.Vpc(this, 'Vpc', {
      ipAddresses: ec2.IpAddresses.cidr(props.cidr ?? MATRIX_VPC_CIDR),
      maxAzs,
      natGateways: 0,
      subnetConfiguration: [{ subnetType: ec2.SubnetType.PRIVATE_ISOLATED, name: 'db', cidrMask: 24 }],
    });
    this.vpc = vpc;

    // One group for the run, and DESTROYed: CDK leaves a subnet group behind by default, and an
    // orphaned group in the subnets keeps the whole VPC from deleting after a run.
    this.databaseSubnetGroup = new rds.SubnetGroup(this, 'DatabaseSubnetGroup', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_ISOLATED },
      description: 'Private subnets of one test-matrix run',
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // The databases start no conversations, so they get no egress; they also take no inbound
    // traffic until a caller says who may reach them (allowDatabasesFrom, used by PR A4).
    this.databaseSecurityGroup = new ec2.SecurityGroup(this, 'DatabaseSecurityGroup', {
      vpc,
      description: 'Managed databases of one test-matrix run',
      allowAllOutbound: false,
    });
  }

  /** Opens `port` on the databases to `source` and nothing else. */
  allowDatabasesFrom(source: ec2.ISecurityGroup, port: ec2.Port, reason: string): void {
    this.databaseSecurityGroup.addIngressRule(source, port, reason);
  }
}
