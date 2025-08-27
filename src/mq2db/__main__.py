from argparse import ArgumentParser
from . import Mq2db


def main():
    argp = ArgumentParser()
    argp.add_argument(
        "path_yaml",
        type=str,
        help="Path to the YAML configuration file.")
    argp.add_argument(
        "--section",
        type=str,
        default=None,
        help=("Section name in the YAML file for mq2db specific configuration. "
              "Something like 'mq2db.specific.setting'."))
    args = argp.parse_args()
    mq2db = Mq2db(args.path_yaml, args.section)
    mq2db.start()
