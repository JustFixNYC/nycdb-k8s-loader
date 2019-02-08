from pathlib import Path
import subprocess
import glob


ROOT_DIR = Path(__file__).parent.parent.resolve()


def test_pyflakes_works():
    subprocess.check_call([
        'pyflakes',
        *glob.glob(str(ROOT_DIR / '*.py')),
        'lib',
        'tests'
    ], cwd=ROOT_DIR)
