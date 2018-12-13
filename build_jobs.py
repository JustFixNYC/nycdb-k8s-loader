import os
from pathlib import Path
from typing import Dict
import yaml

import load_dataset


MY_DIR = Path(__file__).parent.resolve()

JOB_TEMPLATE = MY_DIR / 'job-template.yml'

JOBS_DIR = MY_DIR / 'jobs'


def get_env(name: str) -> Dict[str, str]:
    return {
        'name': name,
        'value': os.environ.get(name, '')
    }


def main():
    load_dataset.sanity_check()
    JOBS_DIR.mkdir(parents=True, exist_ok=True)

    datasets = set([
        table.dataset for table in load_dataset.get_dataset_tables()
    ])

    for dataset in datasets:
        template = yaml.load(JOB_TEMPLATE.read_text())
        c = template['spec']['template']['spec']['containers'][0]
        c['command'] = [
            'python',
            Path(load_dataset.__file__).name,
            dataset
        ]
        c['env'] = [
            get_env('DATABASE_URL'),
            get_env('USE_TEST_DATA')
        ]
        outfile = JOBS_DIR / f'load_dataset_{dataset}.yml'
        print(f"Writing {outfile}.")
        outfile.write_text(yaml.dump(template))
    print("Done!")


if __name__ == '__main__':
    main()
