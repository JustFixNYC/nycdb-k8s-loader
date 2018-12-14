import subprocess
from load_dataset import NYCDB_CMD


if __name__ == '__main__':
    subprocess.call(NYCDB_CMD + ['--dbshell'])
