from nycdb.cli import run_dbshell

from load_dataset import Config


if __name__ == '__main__':
    run_dbshell(Config().nycdb_args)
