import os
from pathlib import Path
from typing import Dict, List
import argparse
import yaml

from scheduling import DATASET_NAMES, get_schedule_for_dataset
import load_dataset


MY_DIR = Path(__file__).parent.resolve()

JOB_TEMPLATE = MY_DIR / "k8s-job-template.yml"

DEFAULT_JOBS_DIR = MY_DIR / "k8s-jobs"

DEFAULT_IMAGE = "justfixnyc/nycdb-k8s-loader:latest"

CONTAINER_ENV_VARS = [
    "DATABASE_URL",
    "USE_TEST_DATA",
    "SLACK_WEBHOOK_URL",
    "ROLLBAR_ACCESS_TOKEN",
    "ALGOLIA_APP_ID",
    "ALGOLIA_API_KEY"
]


def get_env(name: str) -> Dict[str, str]:
    return {"name": name, "value": os.environ.get(name, "")}


def slugify(name: str) -> str:
    return name.replace("_", "-")


def main(args: List[str]):
    load_dataset.sanity_check()

    parser = argparse.ArgumentParser(
        description="Build Kubernetes CronJobs to load NYCDB datasets."
    )
    parser.add_argument(
        "--jobs-dir",
        default=DEFAULT_JOBS_DIR,
        help=f'Directory to write files (default is "{DEFAULT_JOBS_DIR}")',
    )
    parser.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f'Container image to use (default is "{DEFAULT_IMAGE}")',
    )

    pargs = parser.parse_args(args)

    jobs_dir = Path(pargs.jobs_dir)
    jobs_dir.mkdir(parents=True, exist_ok=True)

    for dataset in DATASET_NAMES:
        template = yaml.load(JOB_TEMPLATE.read_text(), Loader=yaml.FullLoader)
        name = template["metadata"]["name"]
        name = f"{name}-{slugify(dataset)}"
        template["metadata"]["name"] = name
        template["spec"]["schedule"] = get_schedule_for_dataset(dataset).k8s
        c = template["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        c["image"] = pargs.image
        c["command"] = ["python", Path(load_dataset.__file__).name, dataset]
        c["env"] = [get_env(varname) for varname in CONTAINER_ENV_VARS]
        outfile = jobs_dir / f"load_dataset_{dataset}.yml"
        print(f"Writing {outfile}.")
        outfile.write_text(yaml.dump(template))
    print("Done!")


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
