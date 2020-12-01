import scads_whitebox
import Utils
import argparse
import logging


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Run placement mapper."
    )
    # parser.add_argument("--nmachines", type=int)
    # parser.add_argument("--nclients", type=int)
    # parser.add_argument("--ranges", type=int)
    # parser.add_argument("--max_processes", type=int)
    # parser.add_argument("--telemetry_filepath", type=str)
    parser.add_argument("--config_filepath", type=str)

    args = parser.parse_args(args=args)
    return args

log_format = "%(asctime)s: %(message)s"
logging.basicConfig(format=log_format, level=logging.DEBUG, datefmt="%H:%M:%S")

args = parse_args(['--config_filepath=nova-tutorial-config'])

Utils.init(args)

scads_whitebox.cfg_change()

