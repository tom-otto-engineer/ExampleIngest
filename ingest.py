import os
import boto3
import logging
import pandas as pd
from dativa.tools.aws import S3Client
from newtools.aws import AthenaPartition
from dativa.scrubber import PersistentFieldLogger, Scrubber
from newtools import DoggoFileSystem, S3Location, AthenaClient, log_to_stdout, PandasDoggo
from datetime import datetime, timedelta

from scrubber_config.scrubber_settings import scrubber_config

logger = log_to_stdout("toms ingest", logging.DEBUG)
stat_logger = PersistentFieldLogger(logger, {"message": ""})


class IngestClass:
    """

    This class will Ingest match data for a given sports tv type
    It will then clean the data according to the scrubber configurations
    Output the data in a S3 location of your choice
    Dynamically create DB, and tv type specific table
    Automatically add missing partitions

    """

    def __init__(self,
                 region,
                 tv_type,
                 database,
                 source_bucket,
                 destination_bucket,
                 table):
        self.region = region
        self.tv_type = tv_type
        self.database = database
        self.ac = AthenaClient(self.region, db=self.database)
        self.source_bucket = S3Location(source_bucket)
        self.int_bucket = S3Location('s3://tv-type-intermediary')
        self.output_location = S3Location(destination_bucket).join(
            f'{self.tv_type}-data')
        self.athena_temp = S3Location('s3://temp-output-query').join('temp')
        self.s3c = S3Client()
        self.boto_client = boto3.client('s3')
        self.table = table
        self.dfs = DoggoFileSystem()
        self.key_map_list = ["day","file"]
        self.sql_path = os.path.join(os.path.dirname(__file__), "sql")

    def clean(self, key):
        try:
            if not key.endswith(".csv"):
                logger.error("File should be in valid .csv format")
            pdg = PandasDoggo()
            df = pdg.load(self.source_bucket.join(key).s3_url)
            df = df[df['Brand'] == self.tv_type]
            dates = df['date'].unique()
            for date in dates:
                pdg.save_csv(df,
                             path=self.int_bucket.join(f'{self.tv_type}-data/{date}/{self.tv_type}.csv'),
                             index=False)
                df['date'] = pd.to_datetime(df.date, format='%Y%m%d')
                stats_dict = dict()
                stats_dict['Maximum'] = max(df['date']).strftime('%Y-%m-%d')
                stats_dict['Minimum'] = min(df['date']).strftime('%Y-%m-%d')
                stats_dict['Unique Record'] = len(df.drop_duplicates())
                stat_logger.info(message="Ingest clean",
                                 max_date=str(stats_dict['Maximum']),
                                 min_date=str(stats_dict['Minimum']),
                                 total_unique_count=str(stats_dict['Unique Record']))
                sc = Scrubber()
                reports = sc.run(df, config=scrubber_config)
                for entry in reports:
                    stat_logger.info(message="Ingest Scrubber clean:" + str(entry))
                return True
        except Exception as e:
            logger.error(str(e))

    def setup(self):
        self.ac.add_query("CREATE DATABASE IF NOT EXISTS {}".format(self.database),
                          name="build database {} if doesnt exist".format(self.database),
                          output_location=self.athena_temp)
        self.ac.wait_for_completion()
        self.create_table()

    def create_table(self):
        sql_path = os.path.join(self.sql_path, "create_table.sql")
        with open(sql_path) as f:
            query = f.read()
        self.ac.add_query(query.format(table=self.table, location=self.output_location),
                          name="build table {}.{} if doesnt exist".format(self.database,
                                                                          self.table),
                          output_location=self.athena_temp)
        self.ac.wait_for_completion()

    def drop_table(self):
        self.ac.add_query("""
                            DROP TABLE IF EXISTS {}
                          """.format(self.table),
                          name="drop table {}.{} if exists".format(self.database, self.table),
                          output_location=self.athena_temp)
        self.ac.wait_for_completion()

    def teardown(self):
        self.drop_table()
        self.ac.add_query("DROP DATABASE IF EXISTS {}".format(self.database),
                          name="drop database {} if exists".format(self.database),
                          output_location=self.athena_temp)
        self.ac.wait_for_completion()
        self.create_table()

    def ingest(self):
        """
        Ingests data, will query for all data we receive each and everytime
        :return:
        """
        try:
            dates = pd.date_range(start=(datetime.utcnow() - timedelta(days=2500)),
                                  end=datetime.utcnow()).tolist()
            date_range = [i.strftime("%Y%m%d") for i in dates]
            delivery_keys = self.s3c.list(self.source_bucket.s3_url, suffix=".csv")
            keys = [i for i in delivery_keys if i.split("/")[0] in [datetime.strptime(i, "%Y%m%d").strftime("%Y%m%d") for i in date_range]]
            if len(keys) > 0:
                for key in keys:
                    key_map = list(zip(self.key_map_list, key.split('/')))
                    key_map = {i[0]: i[1] for i in key_map}
                    date = key.split('/')[0]
                    file_path = f"day={key_map['day']}/{key_map['file']}"
                    flag = self.clean(key)
                    if flag:
                        self.dfs.cp(source=self.int_bucket.join(f'{self.tv_type}-data/{date}/{self.tv_type}.csv'),
                                    destination=self.output_location.join(file_path))
                ap = AthenaPartition(bucket=self.output_location.bucket, s3_client=self.boto_client)
                list_query = ap.get_sql(table=self.table,
                                        s3_path=self.output_location.key,
                                        athena_client=self.ac,
                                        output_location=self.athena_temp)
                for query in list_query:
                    self.ac.add_query(query, output_location=self.athena_temp)
                self.ac.wait_for_completion()
            else:
                logger.error("No key to process")
        except Exception as e:
            logger.error(str(e))


if __name__ == '__main__':
    ingest = IngestClass(region=os.environ.get("REGION"),
                         tv_type=os.environ.get("TV"),
                         database=os.environ.get("DATABASE"),
                         source_bucket=os.environ.get("SOURCE_BUCKET"),
                         destination_bucket=os.environ.get("DEST_BUCKET"),
                         table=os.environ.get("TABLE"))
    ingest.ingest()
