import os
from os import path
from unittest import TestCase
from newtools.doggo import PandasDoggo
from pandas.testing import assert_frame_equal
from ingest import IngestClass


class TestBase(TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestBase, cls).setUpClass()
        cls.base_path = os.path.dirname(os.path.realpath(__file__))

        cls.doggo = PandasDoggo()

    def test_simple(cls):
        ingest = IngestClass(region='us-east-1',
                             tv_type='TCL',
                             database='test_ingest',
                             source_bucket='s3://tv-type-raw/',
                             destination_bucket='s3://tv-type-test/',
                             table='tcl_data_test')
        true_df = cls.doggo.load_csv(path.join(cls.base_path, 'test_data/output/output_data.csv'), dtype=object)
        ingest.setup()
        ingest.ingest()
        output_data = cls.doggo.load_csv(
            path=path.join(ingest.output_location, 'day=20220512/TV_Final.csv'))
        true_df = true_df.astype(str)
        output_data = output_data.astype(str)
        assert_frame_equal(true_df, output_data)
