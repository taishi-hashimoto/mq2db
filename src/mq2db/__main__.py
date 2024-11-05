from argparse import ArgumentParser
from . import Mq2db


def main():
    argp = ArgumentParser()
    argp.add_argument("path_yaml")
    args = argp.parse_args()
    mq2db = Mq2db(args.path_yaml)
    mq2db.start()
