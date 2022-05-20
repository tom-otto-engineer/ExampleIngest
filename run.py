import os
import logging
import configargparse as argparse
from newtools import log_to_stdout
from ingest import IngestClass

parser = argparse.ArgumentParser(description="""Ingests for a given TV, validates data, creates DB + table, 
                                 then partitions cleaned data by day and tv_type""",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 add_config_file_help=False,
                                 add_env_var_help=False,)

parser.add_argument("-tv", "--tv_type", help='tv_type for running the function', required=True, type=str),

parser.add_argument("-so", "--source_bucket", help='source_bucket for running the function', required=True, type=str),

parser.add_argument("-de", "--destination_bucket", help='destination_bucket for running the function', required=True,
                    type=str),

parser.add_argument("-p", "--aws_profile", help="AWS credentials", required=True, env_var='AWS_PROFILE', type=str),

parser.add_argument("-db", "--database", help="the database you want to use", default="ingest", type=str),

parser.add_argument("-t", "--table", help="the table you want to use", required=True, type=str),

parser.add_argument("-r", "--region", help="the region you want to use", default="us-east-1", type=str),

args = vars(parser.parse_args())
os.environ['AWS_PROFILE'] = args["aws_profile"]
ingest = IngestClass(
    region=args['aws_profile'],
    tv_type=args['tv_type'],
    database=args['database'],
    source_bucket=args['source_bucket'],
    destination_bucket=args['destination_bucket'],
    table=args['table'],
)
log_to_stdout("toms ingest", logging.DEBUG)
ingest.ingest()