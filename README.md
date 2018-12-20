[![CircleCI](https://circleci.com/gh/JustFixNYC/nycdb-k8s-loader.svg?style=svg)](https://circleci.com/gh/JustFixNYC/nycdb-k8s-loader)

This repository was created to explore the possibility of
populating a [NYC-DB][] instance via [Kubernetes Jobs][]
or [Amazon Fargate][].

There are a few potential advantages to this approach:

* For developers who are more conversant with containerization,
  it could be more convenient than learning how to
  deploy via a VPS or through tools like Ansible.

* Containerization allows for nice dev/prod parity.

* It potentially parallelizes the workload over multiple machines,
  which could increase the speed of populating the database.

* Kubernetes supports [Cron Jobs][], so even a single-node cluster
  could be used to keep a NYC-DB instance continuously updated,
  with a convenient UI provided via the Kubernetes Dashboard. This
  might be easier to manage than e.g. custom cron jobs on a VPS.
  (Note, however, that at the time of this writing, no tooling is
  provided to help configure Kubernetes Cron Jobs.)

  Amazon Fargate supports [Scheduled Tasks][], so a NYC-DB can be
  continuously updated on AWS infrastructure as well, though
  at the time of this writing it has a number of limitations
  compared to the Kubernetes approach. However, unlike Kubernetes,
  it also doesn't require any kind of master server, so it
  could be less expensive to maintain. A script is
  also included in this repository to help configure such tasks.

## Setup

You will need Docker.

First, you'll want to create an `.env` file by copying the example one:

```
cp .env.example .env     # Or 'copy .env.example .env' on Windows
```

Take a look at the `.env` file and make any changes to it, if you like.

## Development

The easiest way to develop the loader is via Docker Compose, as it sets up
a development Postgres server for you.

Enter the development container by running:

```
docker-compose run app sh
```

You can now develop and run the `load_dataset.py` script. The `/app`
directory is mounted to the root of the repository on your local filesystem,
so any changes you make to any files will instantly be reflected in the
container environment.

## Deployment: Kubernetes

You'll need a Kubernetes (k8s) cluster. The easiest way to
obtain one on your local machine is by
[enabling Kubernetes on Docker Desktop][enable-k8s].

You may also want to deploy the [Kubernetes Dashboard UI][], as it makes
introspecting the state of the NYC-DB dataset loader jobs very easy.

You'll want to build your container image by running:

```
docker-compose build
```

To deploy the jobs to your Kubernetes cluster, first generate job files:

```
docker-compose run app python build_jobs.py
```

Note that the created jobs will use the environment variables defined
in your `.env` file.

Then tell k8s to start your jobs:

```
kubectl create -f ./jobs
```

Now you can visit the "Jobs" section of your Kubernetes Dashboard to see
the state of the jobs.

If you want to stop the jobs, or clean them up once they're finished, run:

```
kubectl delete -f ./jobs
```

## Deployment: Amazon Fargate

It's also possible to deploy this container as a Task on Amazon Fargate,
which supports scheduled tasks. Here are some guidelines:

* The "get started" wizard for Fargate has you set up a Service
  definition, but you won't need this particular feature, because
  you're not setting up a web server or anything. You should be
  able to safely delete the Service after you're done with the
  wizard.

* You can set your Task's container image to
  [`justfixnyc/nycdb-k8s-loader:latest`][] and set the environment
  variables as per the documentation in the
  [`.env.example`](.env.example) file.

* When running the Task, you'll want to set "Auto-assign public IP"
  to `ENABLED`: even though the container doesn't need to be
  accessed *by* anything, apparently your container needs a public IP
  in order to access the outside world (see
  [aws/amazon-ecs-agent#1128][] for more details). Note also that
  this setting is part of _running_ a Task rather than _defining_
  a Task.

* Make sure your DB instance is accessible by the container in your
  VPC. There can be situations where your DB might be accessible from
  the internet, yet your container times out when attempting to
  access it. This could be because the security group for your
  RDS instance has inbound rules that only allow access from a
  certain IP range. (If you created an RDS instance via the AWS
  Console, its security group might be called `rds-launch-wizard`.)

* At the time of this writing, it's not possible to see task-level
  CPU/memory utilization in CloudWatch, which is unfortunate (see
  [aws/amazon-ecs-agent#565](https://github.com/aws/amazon-ecs-agent/issues/565)).

* At the time of this writing, it's not possible to use a SSM parameter
  store as a secret store (see
  [aws/amazon-ecs-agent#1209](https://github.com/aws/amazon-ecs-agent/issues/1209)).
  This means you will probably need to specify your database URL as
  plaintext in your task definition.

To create scheduled tasks for loading each dataset on a regular basis,
see [`aws_schedule_tasks.py`](aws_schedule_tasks.py).

## Deployment: other systems

The container can be configured through environment variables,
so take a look at the [`.env.example`](.env.example) file for
documentation on all of them.

## How it works

The loader works by creating a temporary [Postgres schema][]
for a dataset, and loading the dataset into that schema, which
could take a long time. Using the temporary schema ensures that
users can still make queries to the public schema (if one exists)
while the new version of the dataset is being loaded.

Once the dataset has been loaded into the temporary schema,
the loader drops the dataset's tables from the public schema
and moves the temporary schema's tables into the public schema.

The loader also tries to ensure that users have the same
permissions to the new tables in the public schema that they
had to the old tables. However, you should probably verify
this manually.

## Querying load status

If you want to get an idea of how loading is going without viewing logs,
you could use the [`show_rowcounts.py`](show_rowcounts.py) utility.

## Tests

To run the test suite, run:

```
docker-compose run app pytest
```

## Updating the NYC-DB version

At present, the revision of NYC-DB's Python library is pulled directly
from GitHub via a commit hash.  At the time of this writing, that
commit hash is specified with the [`Dockerfile`'s `NYCDB_REV` argument][rev].

To update the revision for anyone who is using the
[`justfixnyc/nycdb-k8s-loader:latest`][] image off Docker Hub, issue a PR
that changes the default value of the aforementioned `NYCDB_REV` argument.
Our continuous integration system will then ensure that everything still
works, and once the PR is merged into `master`, Docker Hub will re-publish
a new container image that uses the latest version of NYC-DB.

[Cron Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
[NYC-DB]: https://github.com/aepyornis/nyc-db
[Kubernetes Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
[enable-k8s]: https://docs.docker.com/docker-for-windows/#kubernetes
[Kubernetes Dashboard UI]: https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/#deploying-the-dashboard-ui
[Amazon Fargate]: https://aws.amazon.com/fargate/
[`justfixnyc/nycdb-k8s-loader:latest`]: https://hub.docker.com/r/justfixnyc/nycdb-k8s-loader
[aws/amazon-ecs-agent#1128]: https://github.com/aws/amazon-ecs-agent/issues/1128#issuecomment-351545461
[Scheduled Tasks]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/scheduled_tasks.html
[rev]: https://github.com/JustFixNYC/nycdb-k8s-loader/blob/master/Dockerfile#L19
[Postgres schema]: https://www.postgresql.org/docs/9.5/ddl-schemas.html
