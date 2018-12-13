This repository was created to explore the possibility of
populating a [NYC-DB][] instance via [Kubernetes Jobs][].

The potential advantage of this is that it parallelizes the
workload over multiple machines, which could increase the
speed of populating the database.

Furthermore, while it hasn't yet been explored at the
time of this writing, Kubernetes also supports [Cron Jobs][],
so even a single-node cluster could be used to keep a
NYC-DB instance continuously updated.

## Quick start

You will need Docker, and a Kubernetes (k8s) cluster. The easiest way to
obtain the latter is by [enabling Kubernetes on Docker Desktop][enable-k8s].

You may also want to deploy the [Kubernetes Dashboard UI][], as it makes
introspecting the state of the NYC-DB dataset loader jobs very easy.

First, you'll want to create an `.env` file by copying the example one:

```
cp .env.example .env     # Or 'copy .env.examplpe .env' on Windows
```

Take a look at the `.env` file and make any changes to it, if you like.

### Development

The easiest way to develop the loader is via Docker Compose, as it sets up
a development Postgres server for you.

Enter the development container by running:

```
docker-compose run app bash
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

[Cron Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/
[NYC-DB]: https://github.com/aepyornis/nyc-db
[Kubernetes Jobs]: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/
[enable-k8s]: https://docs.docker.com/docker-for-windows/#kubernetes
[Kubernetes Dashboard UI]: https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/#deploying-the-dashboard-ui
