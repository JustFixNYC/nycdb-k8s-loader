import os
from pathlib import Path
from typing import Dict
import yaml

from scheduling import DATASET_NAMES, get_schedule_for_dataset
import load_dataset


MY_DIR = Path(__file__).parent.resolve()

JOB_TEMPLATE = MY_DIR / 'k8s-job-template.yml'

JOBS_DIR = MY_DIR / 'k8s-jobs'


def get_env(name: str) -> Dict[str, str]:
    return {
        'name': name,
        'value': os.environ.get(name, '')
    }


def slugify(name: str) -> str:
    return name.replace('_', '-')


def main(jobs_dir: Path=JOBS_DIR):
    load_dataset.sanity_check()
    jobs_dir.mkdir(parents=True, exist_ok=True)

    for dataset in DATASET_NAMES:
        template = yaml.load(
            JOB_TEMPLATE.read_text(),
            Loader=yaml.FullLoader
        )
        name = template['metadata']['name']
        name = f"{name}-{slugify(dataset)}"
        template['metadata']['name'] = name
        template['spec']['schedule'] = get_schedule_for_dataset(dataset).k8s
        c = template['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]
        c['command'] = [
            'python',
            Path(load_dataset.__file__).name,
            dataset
        ]
        c['env'] = [
            get_env('DATABASE_URL'),
            get_env('USE_TEST_DATA')
        ]
        outfile = jobs_dir / f'load_dataset_{dataset}.yml'
        print(f"Writing {outfile}.")
        outfile.write_text(yaml.dump(template))
    print("Done!")


if __name__ == '__main__':
    main()
