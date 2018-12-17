"""\
Create scheduled tasks to populate NYC-DB tables.

Usage:
  aws_schedule_tasks.py create [--use-test-data]
  aws_schedule_tasks.py delete

Options:
  -h --help     Show this screen.

Environment variables:
  AWS_ACCESS_KEY_ID      Your AWS access key ID.
  AWS_SECRET_ACCESS_KEY  Your AWS secret access key.

This tool currently has a lot of limitations, as it
expects the following to already be set up for it:

  * an IAM role allowing CloudWatch events to start
    ECS tasks.
  * an ECS cluster.
  * a VPC with a subnet for the ECS task.
  * a task definition, with all environment variables
    pre-filled except for DATASET and USE_TEST_DATA.

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

# The name of the container within the ECS task definition
# that is responsible for loading NYC-DB datasets.
CONTAINER_NAME = 'nycdb-k8s-loader'

# Various schedule expressions.
#
# For more details on schedule expressions, see:
#
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
DAILY = 'rate(1 day)'
MONTHLY = 'rate(30 days)'
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


def create_input_str(dataset: str, use_test_data: bool):
    '''
    Create the JSON-encoded input string that specifies
    the environment variables to pass to the dataset-loading
    container.
    '''

    return json.dumps({
        'containerOverrides': [{
            'name': CONTAINER_NAME,
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


def find_subnet() -> str:
    ec2 = boto3.resource('ec2')
    for vpc in ec2.vpcs.all():
        for subnet in vpc.subnets.all():
            return subnet.id

    raise AssertionError("No subnets found!")


def create_task(
    dataset: str,
    use_test_data: bool,
    role_arn: str,
    cluster_arn: str,
    task_arn: str,
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
    input_str = create_input_str(dataset=dataset, use_test_data=use_test_data)
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

    print(f"Obtaining cluster information.")
    ecs = boto3.client('ecs')
    cluster_arn = ecs.list_clusters()['clusterArns'][0]
    print(f"Found cluster {cluster_arn}.")

    print(f"Obtaining task definition for {TASK_DEFINITION_NAME}.")
    task_arn = ecs.describe_task_definition(
        taskDefinition=TASK_DEFINITION_NAME)['taskDefinition']['taskDefinitionArn']
    print(f"Found {task_arn}.")

    print(f"Obtaining subnet information.")
    subnet = find_subnet()
    print(f"Found subnet {subnet}.")

    for dataset in DATASET_NAMES:
        create_task(
            dataset=dataset,
            use_test_data=args['--use-test-data'],
            role_arn=role_arn,
            cluster_arn=cluster_arn,
            task_arn=task_arn,
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
