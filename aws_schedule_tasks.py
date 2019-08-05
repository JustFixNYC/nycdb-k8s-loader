"""\
Manage scheduled tasks to populate NYC-DB tables.

Usage:
  aws_schedule_tasks.py create <cluster-name>
    [--task-prefix=<prefix>]
    [--task-definition=<name>]
    [--ecs-events-role=<role>]
    [--use-test-data]

  aws_schedule_tasks.py delete [--task-prefix=<prefix>]

Options:
  -h --help                 Show this screen.
  --task-prefix=<prefix>    The prefix to prepend all scheduled task
                            names/rules with. This will be followed
                            by the dataset name [default: nycdb-load-].
  --task-definition=<name>  The name of the pre-existing ECS task
                            definition to load NYC-DB datasets. It
                            should have a single container that has
                            all environment variables pre-filled
                            except for DATASET and USE_TEST_DATA
                            [default: nycdb-k8s-loader].
  --ecs-events-role=<role>  The pre-existing IAM role that allows
                            CloudWatch events to start ECS tasks
                            [default: ecsEventsRole].
  --use-test-data           Make the dataset loading tasks
                            load small test datasets instead of the
                            real thing.
  <cluster-name>            The name of the pre-existing ECS
                            cluster that the scheduled tasks will
                            be in.

Environment variables:
  AWS_ACCESS_KEY_ID      Your AWS access key ID.
  AWS_SECRET_ACCESS_KEY  Your AWS secret access key.
  AWS_DEFAULT_REGION     The AWS region to use.
"""

from typing import List, Dict
import json
import boto3
import docopt
import dotenv
import nycdb.dataset

dotenv.load_dotenv()

# The names of all valid NYC-DB datasets.
DATASET_NAMES: List[str] = list(nycdb.dataset.datasets().keys())

# Various schedule expressions. Note that all times must be specified
# in UTC.
#
# For more details on schedule expressions, see:
#
# https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html
DAILY = 'cron(0 5 * * ? *)'               # Daily at around midnight EST.
EVERY_OTHER_DAY = 'cron(0 5 */2 * ? *)'   # Every other day around midnight EST.
YEARLY = 'rate(365 days)'

# The default schedule expression for a dataset loader, if
# otherwise unspecified.
DEFAULT_SCHEDULE_EXPRESSION = YEARLY

DATASET_SCHEDULES: Dict[str, str] = {
    'dobjobs': DAILY,
    'dob_complaints': DAILY,
    'dob_violations': DAILY,
    'ecb_violations': DAILY,
    'hpd_violations': DAILY,
    'oath_hearings': DAILY,
    'hpd_registrations': EVERY_OTHER_DAY,
    'hpd_complaints': EVERY_OTHER_DAY,
    'dof_sales': EVERY_OTHER_DAY,
    'pad': EVERY_OTHER_DAY,
    'acris': EVERY_OTHER_DAY
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


def delete_tasks(prefix: str):
    '''
    Delete the scheduled tasks with the given prefix, if they exist.
    '''

    client = boto3.client('events')
    response = client.list_rules(NamePrefix=prefix)
    for rule in response['Rules']:
        name = rule['Name']
        targets = client.list_targets_by_rule(Rule=name)['Targets']
        print(f"Deleting rule '{name}'.")
        client.remove_targets(Rule=name, Ids=[
            target['Id'] for target in targets
        ])
        client.delete_rule(Name=name)


def create_task(
    prefix: str,
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

    name = f"{prefix}{dataset}"
    schedule_expression = DATASET_SCHEDULES.get(dataset, DEFAULT_SCHEDULE_EXPRESSION)

    print(f"Creating rule '{name}' with schedule {schedule_expression}.")
    client = boto3.client('events')
    client.put_rule(
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


def create_tasks(prefix: str, args):
    '''
    Create or update the scheduled tasks.
    '''

    ecs_events_role: str = args['--ecs-events-role']
    print(f"Obtaining ARN for role '{ecs_events_role}'")
    iam = boto3.client('iam')
    role_arn = iam.get_role(RoleName=ecs_events_role)['Role']['Arn']

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

    task_definition: str = args['--task-definition']
    print(f"Obtaining task definition for {task_definition}.")
    task = ecs.describe_task_definition(
        taskDefinition=task_definition)['taskDefinition']
    task_arn = task['taskDefinitionArn']
    container_name = task['containerDefinitions'][0]['name']
    print(f"Found {task_arn} with container {container_name}.")

    use_test_data: bool = args['--use-test-data']

    for dataset in DATASET_NAMES:
        create_task(
            prefix=prefix,
            dataset=dataset,
            use_test_data=use_test_data,
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
    prefix: str = args['--task-prefix']

    if args['create']:
        create_tasks(prefix, args)
    elif args['delete']:
        delete_tasks(prefix)


if __name__ == '__main__':
    main()
