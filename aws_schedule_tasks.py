"""\
Create scheduled tasks to populate NYC-DB tables.

Usage:
  aws_schedule_tasks.py create <cluster-name> [--use-test-data]
  aws_schedule_tasks.py delete

Options:
  -h --help     Show this screen.

Environment variables:
  AWS_ACCESS_KEY_ID      Your AWS access key ID.
  AWS_SECRET_ACCESS_KEY  Your AWS secret access key.

This tool currently has a lot of limitations, as it
expects the following to already be set up for it:

  * an ECS cluster, passed in via the command-line.
  * an IAM role allowing CloudWatch events to start
    ECS tasks, specified by the ECS_EVENTS_ROLE
    constant in this script.
  * a task definition, with a single container that
    has all environment variables pre-filled except
    for DATASET and USE_TEST_DATA. The name of the task
    is specified by TASK_DEFINITION_NAME in this script.

In other words, this tool is only responsible for
managing CloudWatch rules to schedule the dataset-loading
tasks; everything else should already exist.
"""

import sys
from typing import List, Dict
import json
import boto3
import docopt
import dotenv
import nycdb.datasets

dotenv.load_dotenv()

# The names of all valid NYC-DB datasets.
DATASET_NAMES: List[str] = list(nycdb.datasets.datasets().keys())

# The AWS region to use.
REGION_NAME = 'us-east-1'

# The prefix of the rules we'll create (one per dataset).
PREFIX = "nycdb-load-"

# The IAM role that allows CloudWatch events to start
# ECS tasks.
ECS_EVENTS_ROLE = 'ecsEventsRole'

# The name of the ECS task definition to load NYC-DB datasets.
TASK_DEFINITION_NAME = 'nycdb-k8s-loader'

# Various schedule expressions.
#
# Because Amazon uses a weird cron format without any way to
# convert them into plain language, who knows if these are correct.
#
# For more details on schedule expressions, see:
#
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
DAILY = 'cron(0 0 * * ? *)'     # Daily at midnight, I think?
MONTHLY = 'cron(0 0 1 * ? *)'   # The first of every month at midnight, maybe?
YEARLY = 'rate(365 days)'

# The default schedule expression for a dataset loader, if
# otherwise unspecified.
DEFAULT_SCHEDULE_EXPRESSION = YEARLY

DATASET_SCHEDULES: Dict[str, str] = {
    'dobjobs': DAILY,
    'dob_complaints': DAILY,
    'hpd_violations': DAILY,
    'oath_hearings': DAILY,
    'hpd_registrations': MONTHLY,
    'hpd_complaints': MONTHLY,
    'dof_sales': MONTHLY,
    'acris': MONTHLY
}


def create_input_str(container_name: str, dataset: str, use_test_data: bool):
    '''
    Create the JSON-encoded input string that specifies
    the environment variables to pass to the given dataset-loading
    container.
    '''

    return json.dumps({
        'containerOverrides': [{
            'name': container_name,
            'environment': [{
                'name': 'DATASET',
                'value': dataset
            }, {
                'name': 'USE_TEST_DATA',
                'value': 'yup' if use_test_data else ''
            }]
        }]
    })


def delete_tasks():
    '''
    Delete the scheduled tasks, if they exist.
    '''

    client = boto3.client('events')
    response = client.list_rules(NamePrefix=PREFIX)
    for rule in response['Rules']:
        name = rule['Name']
        targets = client.list_targets_by_rule(Rule=name)['Targets']
        print(f"Deleting rule '{name}'.")
        client.remove_targets(Rule=name, Ids=[
            target['Id'] for target in targets
        ])
        client.delete_rule(Name=name)


def create_task(
    dataset: str,
    use_test_data: bool,
    role_arn: str,
    cluster_arn: str,
    task_arn: str,
    container_name: str,
    subnet: str
):
    '''
    Create a scheduled task for the given dataset.
    '''

    name = f"{PREFIX}{dataset}"
    schedule_expression = DATASET_SCHEDULES.get(dataset, DEFAULT_SCHEDULE_EXPRESSION)

    print(f"Creating rule '{name}' with schedule {schedule_expression}.")
    client = boto3.client('events')
    response = client.put_rule(
        Name=name,
        ScheduleExpression=schedule_expression,
        State="ENABLED",
        Description=f"Load the dataset '{dataset}' into NYC-DB.",
        RoleArn=role_arn
    )

    target_id = dataset
    input_str = create_input_str(
        container_name=container_name,
        dataset=dataset,
        use_test_data=use_test_data
    )
    print(f"Creating target '{target_id}'.")
    client.put_targets(
        Rule=name,
        Targets=[{
            "Id": target_id,
            "Arn": cluster_arn,
            "RoleArn": role_arn,
            "Input": input_str,
            "EcsParameters": {
                "TaskDefinitionArn": task_arn,
                "TaskCount": 1,
                "LaunchType": "FARGATE",
                "NetworkConfiguration": {
                    "awsvpcConfiguration": {
                        "Subnets": [subnet],
                        "SecurityGroups": [],
                        "AssignPublicIp": "ENABLED"
                    }
                },
                "PlatformVersion": "LATEST"
            }
        }]
    )


def create_tasks(args):
    '''
    Create or update the scheduled tasks.
    '''

    print(f"Obtaining ARN for role '{ECS_EVENTS_ROLE}'")
    iam = boto3.client('iam')
    role_arn = iam.get_role(RoleName=ECS_EVENTS_ROLE)['Role']['Arn']

    cluster_name: str = args['<cluster-name>']
    print(f"Obtaining cluster information for {cluster_name}.")
    ecs = boto3.client('ecs')
    clusters = ecs.describe_clusters(clusters=[cluster_name])
    cluster_arn: str = clusters['clusters'][0]['clusterArn']
    print(f"Found cluster {cluster_arn}.")

    print(f"Obtaining VPC and subnet info for cluster {cluster_name}.")
    ec2 = boto3.resource('ec2')
    vpc = list(ec2.vpcs.filter(Filters=[{
        'Name': 'tag:Name',
        'Values': [f'ECS {cluster_name} - VPC']
    }]).all())[0]
    subnet: str = list(vpc.subnets.all())[0].id
    print(f"Found VPC {vpc.id} and subnet {subnet}.")

    print(f"Obtaining task definition for {TASK_DEFINITION_NAME}.")
    task = ecs.describe_task_definition(
        taskDefinition=TASK_DEFINITION_NAME)['taskDefinition']
    task_arn = task['taskDefinitionArn']
    container_name = task['containerDefinitions'][0]['name']
    print(f"Found {task_arn} with container {container_name}.")

    for dataset in DATASET_NAMES:
        create_task(
            dataset=dataset,
            use_test_data=args['--use-test-data'],
            role_arn=role_arn,
            cluster_arn=cluster_arn,
            task_arn=task_arn,
            container_name=container_name,
            subnet=subnet
        )


def sanity_check():
    for dataset in DATASET_SCHEDULES:
        assert dataset in DATASET_NAMES


def main():
    sanity_check()

    args = docopt.docopt(__doc__)

    boto3.setup_default_session(region_name=REGION_NAME)

    if args['create']:
        create_tasks(args)
    elif args['delete']:
        delete_tasks()


if __name__ == '__main__':
    main()
