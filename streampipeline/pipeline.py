
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
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms import trigger
import joblib
import pandas as pd
def timestamp2str(t, fmt='%Y-%m-%d %H:%M:%S.000'):
    """Converts a unix timestamp into a formatted string."""
    return datetime.fromtimestamp(t).strftime(fmt)


class ParseFn(beam.DoFn):
    """Parses the raw game event info into a Python dictionary.

    Each event line has the following format:
        Setting_0,Setting_1,Setting_2,Sensor_0,Sensor_1,Sensor_2,Sensor_3,Sensor_4,Sensor_5,Sensor_6,Sensor_7,Sensor_8,Sensor_9,Sensor_10,Sensor_11,Sensor_12,Sensor_13,Sensor_14,Sensor_15,Sensor_16,Sensor_17,Sensor_18,Sensor_19,Sensor_20,Timestamp


    e.g.:
        0,1,0.0023,0.0003,100.0,518.67,643.02,1585.29,1398.21,14.62,21.61,553.9,2388.04,9050.17,1.3,47.2,521.72,2388.03,8125.55,8.4052,0.03,392,2388,100.0,38.86,23.3735,1603961200.0

    The human-readable time string is not used here.
    """

    def __init__(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(ParseGameEventFn, self).__init__()
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]
            yield {
                'Setting_0': row[0],
                'Setting_1': row[1],
                'Setting_2': row[2],
                'Sensor_0': row[3],
                'Sensor_1': row[4],
                'Sensor_2': row[5],
                'Sensor_3': row[6],
                'Sensor_4': row[7],
                'Sensor_5': row[8],
                'Sensor_6': row[9],
                'Sensor_7': row[10],
                'Sensor_8': row[11],
                'Sensor_9': row[12],
                'Sensor_10': row[13],
                'Sensor_11': row[14],
                'Sensor_12': row[15],
                'Sensor_13': row[16],
                'Sensor_14': row[17],
                'Sensor_15': row[18],
                'Sensor_16': row[19],
                'Sensor_17': row[20],
                'Sensor_18': row[21],
                'Sensor_19': row[22],
                'Sensor_20': row[23],
                'Timestamp': row[24],
            }
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)





def remove_novariance(data):
    X = pd.DataFrame.from_dict(data)

    # Fit the feature selection method
    variance_selector= joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020labs97/preproces_models/variance_selector.joblib'))

    # Apply selector on training data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns = X.columns.values[columns_variance])


    yield X #convert.to_pcollection(df)

class MyPredictDoFn(beam.DoFn):

    def process(self, data):
        model = joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020labs97/ml_models/model.joblib'))
        result = model.predict(data)
        results = {'timestamp': data['timestamp'],
                   'RUL': result
                   }

        return results

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

    # Enforce that this pipeline is always run in streaming mode
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        # Read game events from Pub/Sub using custom timestamps, which are extracted
        # from the pubsub data elements, and parse the data.

        # Read from PubSub into a PCollection.

        data = (p | 'ReadPubSub' >> beam.io.ReadFromPubSub(
            subscription=args.subscription)
                | 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
                | 'ParseGameEventFn' >> beam.ParDo(ParseFn())
                | 'Remove_Variance' >> beam.FlatMap(remove_novariance))

        output = (data | 'Predict' >> beam.ParDo(MyPredictDoFn()))
        output | 'WriteTeamScoreSums' >> WriteToBigQuery(
            args.table_name,
            args.dataset,
            {
                'timestamp': 'INTEGER',
                'RUL': 'INTEGER',

            }, options.view_as(GoogleCloudOptions).project)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()