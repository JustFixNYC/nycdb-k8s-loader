from pathlib import Path
import tempfile

import k8s_build_jobs


def test_build_jobs_works():
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmp = Path(tmpdirname)
        k8s_build_jobs.main(tmp)
        assert (tmp / 'load_dataset_hpd_registrations.yml').exists()
