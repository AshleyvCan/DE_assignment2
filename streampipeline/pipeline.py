
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import csv
import logging
import sys
import time
from datetime import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.transforms import trigger
import joblib
import pandas as pd
import numpy as np

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'

def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)



def parse(elem):
        row = list(csv.reader([elem]))[0]
        return {
            'Setting_0': [float(row[0])],
            'Setting_1': [float(row[1])],
            'Setting_2': [float(row[2])],
            'Sensor_0': [float(row[3])],
            'Sensor_1': [float(row[4])],
            'Sensor_2': [float(row[5])],
            'Sensor_3': [float(row[6])],
            'Sensor_4': [float(row[7])],
            'Sensor_5': [float(row[8])],
            'Sensor_6': [float(row[9])],
            'Sensor_7': [float(row[10])],
            'Sensor_8': [float(row[11])],
            'Sensor_9': [float(row[12])],
            'Sensor_10': [float(row[13])],
            'Sensor_11': [float(row[14])],
            'Sensor_12': [float(row[15])],
            'Sensor_13': [float(row[16])],
            'Sensor_14': [float(row[17])],
            'Sensor_15': [float(row[18])],
            'Sensor_16': [float(row[19])],
            'Sensor_17': [float(row[20])],
            'Sensor_18': [float(row[21])],
            'Sensor_19': [float(row[22])],
            'Sensor_20': [float(row[23])],
            'timestamp': [int(float(row[24]))],
        }






def remove_novariance(data):
    df = pd.DataFrame(data)
    X = df.loc[:, df.columns != 'timestamp']
    # Fit the feature selection method
    variance_selector = joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020assignment2/preproces_models/variance_selector.joblib'))

    # Apply selector on training data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns = X.columns.values[columns_variance])
    X = pd.concat([X, df['timestamp']], axis =1)
    logging.getLogger().setLevel(logging.INFO)
    logging.info(X)

    return X #convert.to_pcollection(df)

class MyPredictDoFn(beam.DoFn):

    def process(self, element, **kwargs):
        model = joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020assignment2/ml_models/model.joblib'))
        df = pd.DataFrame(element)
        X = df.loc[:, df.columns != 'timestamp']
        result = model.predict(X)
        results = {'timestamp': int(df['timestamp'].values[0]),
                   'RUL': int(result)
                   }
        logging.getLogger().setLevel(logging.INFO)
        logging.info(X)
        logging.info(results)


        yield results

class PredictWindows(beam.PTransform):
    """Extract user/score pairs from the event stream using processing time, via
    global windowing. Get periodic updates on all users' running scores.
    """

    def __init__(self, allowed_lateness):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(CalculateUserScores, self).__init__()
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        return (
                pcoll
                # Get periodic results every ten events.
                | 'GlobalWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)
                # Extract and sum username/score pairs from the event data.
                | 'Prediction' >> beam.ParDo(MyPredictDoFn()))

class DecodeWindows(beam.PTransform):
    """Extract user/score pairs from the event stream using processing time, via
    global windowing. Get periodic updates on all users' running scores.
    """

    def __init__(self, allowed_lateness):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(CalculateUserScores, self).__init__()
        beam.PTransform.__init__(self)
        self.allowed_lateness_seconds = allowed_lateness * 60

    def expand(self, pcoll):
        return (
                pcoll
                # Get periodic results every ten events.
                | 'DecodeWindows' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(10)),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=self.allowed_lateness_seconds)
                # Extract and sum username/score pairs from the event data.
                | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8')))


class WriteToBigQuery(beam.PTransform):
    """Generate, format, and write BigQuery table row information."""

    def __init__(self, table_name, dataset, schema, project):
        """Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
          project: Name of the Cloud project containing BigQuery table.
        """
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(WriteToBigQuery, self).__init__()
        beam.PTransform.__init__(self)
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """Build the output table schema."""
        return ', '.join('%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, pcoll):
        return (
                pcoll
                | 'ConvertToRow' >>
                beam.Map(lambda elem: {col: elem[col]
                                       for col in self.schema})
                | beam.io.WriteToBigQuery(
            self.table_name, self.dataset, self.project, self.get_schema()))


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the hourly_team_score pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--topic', type=str, help='Pub/Sub topic to read from')
    parser.add_argument(
        '--subscription', type=str, help='Pub/Sub subscription to read from')
    parser.add_argument(
        '--dataset',
        type=str,
        required=True,
        help='BigQuery Dataset to write tables to. '
             'Must already exist.')
    parser.add_argument(
        '--table_name',
        default='results',
        help='The BigQuery table name. Should not already exist.')
    parser.add_argument(
        '--allowed_lateness',
        type=int,
        default=1,
        help='Numeric value of allowed data lateness, in minutes')

    args, pipeline_args = parser.parse_known_args(argv)

    if args.topic is None and args.subscription is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: one of --topic or --subscription is required')
        sys.exit(1)

    options = PipelineOptions(pipeline_args)

    # We also require the --project option to access --dataset
    if options.view_as(GoogleCloudOptions).project is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project is required')
        sys.exit(1)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = save_main_session
    logging.getLogger().setLevel(logging.INFO)
    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read game events from Pub/Sub using custom timestamps, which are extracted
        # from the pubsub data elements, and parse the data.

        # Read from PubSub into a PCollection.

        data = (p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            subscription=args.subscription)
                | 'Window' >> beam.WindowInto(window.FixedWindows(30))
                | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
                | 'ParsFn' >> beam.Map(parse)
                | 'Remove_Variance' >> beam.Map(remove_novariance)
                | 'Predict' >> beam.ParDo(MyPredictDoFn()))

        (data | 'WriteToBQ' >> WriteToBigQuery(
                    args.table_name,
                    args.dataset,
                    {
                        'timestamp': 'INTEGER',
                        'RUL': 'INTEGER',

                    }, options.view_as(GoogleCloudOptions).project))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()