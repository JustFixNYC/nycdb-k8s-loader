from pathlib import Path
import tempfile

import build_jobs


def test_build_jobs_works():
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp = Path(tmpdirname)
        build_jobs.main(tmp)
        assert (tmp / 'load_dataset_hpd_registrations.yml').exists()
