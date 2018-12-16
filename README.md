[![CircleCI](https://circleci.com/gh/JustFixNYC/nycdb-k8s-loader.svg?style=svg)](https://circleci.com/gh/JustFixNYC/nycdb-k8s-loader)

This repository was created to explore the possibility of
populating a [NYC-DB][] instance via [Kubernetes Jobs][]
or [Amazon Fargate][].

The potential advantage of this is that it parallelizes the
workload over multiple machines, which could increase the
speed of populating the database.

Furthermore, while it hasn't yet been explored at the
time of this writing, Kubernetes also supports [Cron Jobs][],
so even a single-node cluster could be used to keep a
NYC-DB instance continuously updated. Amazon Fargate supports
[Scheduled Tasks][], so a NYC-DB could be continuously
updated on AWS infrastructure as well.

## Quick start

You will need Docker, and a Kubernetes (k8s) cluster. The easiest way to
obtain the latter is by [enabling Kubernetes on Docker Desktop][enable-k8s].

You may also want to deploy the [Kubernetes Dashboard UI][], as it makes
introspecting the state of the NYC-DB dataset loader jobs very easy.

First, you'll want to create an `.env` file by copying the example one:

```
cp .env.example .env     # Or 'copy .env.example .env' on Windows
```

Take a look at the `.env` file and make any changes to it, if you like.

### Development

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

### Deployment

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

## Querying load status

If you want to get an idea of how loading is going without viewing logs,
you could use the [`show_rowcounts.py`](show_rowcounts.py) utility.

## Using Amazon Fargate

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

To create scheduled tasks for loading each dataset on a regular basis,
see [`aws_schedule_tasks.py`](aws_schedule_tasks.py).

## Tests

To run the test suite, run:

```
docker-compose run app pytest
```


[Cron Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
[NYC-DB]: https://github.com/aepyornis/nyc-db
[Kubernetes Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
[enable-k8s]: https://docs.docker.com/docker-for-windows/#kubernetes
[Kubernetes Dashboard UI]: https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/#deploying-the-dashboard-ui
[Amazon Fargate]: https://aws.amazon.com/fargate/
[`justfixnyc/nycdb-k8s-loader:latest`]: https://hub.docker.com/r/justfixnyc/nycdb-k8s-loader
[aws/amazon-ecs-agent#1128]: https://github.com/aws/amazon-ecs-agent/issues/1128#issuecomment-351545461
[Scheduled Tasks]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/scheduled_tasks.html
