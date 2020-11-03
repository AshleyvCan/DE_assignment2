from __future__ import absolute_import

import argparse
import csv
import io
import json
import logging

import grpc
import apache_beam as beam
import pandas as pd
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from sklearn import neighbors, metrics
from google.cloud import storage
import joblib

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'

# Predict and Test ML model
def test_model(gs_data, project_id, bucket_name):

    # Read the csv file
    gs_data = beam.io.filesystems.FileSystems.open(gs_data)
    headers = ['Setting_0','Setting_1','Setting_2','Sensor_0','Sensor_1','Sensor_2','Sensor_3','Sensor_4','Sensor_5',
               'Sensor_6','Sensor_7','Sensor_8','Sensor_9','Sensor_10','Sensor_11','Sensor_12','Sensor_13','Sensor_14',
               'Sensor_15','Sensor_16','Sensor_17','Sensor_18','Sensor_19','Sensor_20','RUL']
    df = pd.read_csv(io.TextIOWrapper(gs_data), index_col = 0, names = headers)


    X = df.loc[:, df.columns != 'RUL']
    Y = df['RUL']

    # Load variance_selector
    variance_selector = joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020assignment2/preproces_models/variance_selector.joblib'))

    # Apply the feature selection method to the data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns=X.columns.values[columns_variance])

    # Load of ML model
    knn_model = joblib.load(beam.io.filesystems.FileSystems.open('gs://de2020assignment2/ml_models/model.joblib'))

    # Evaluate model performance on validation data
    MAE_score = metrics.mean_absolute_error(Y, knn_model.predict(X))
    print(MAE_score)
    return json.dumps('MAE_score: '+ str(MAE_score), sort_keys=False, indent=4)


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://de2020assignment2//preprocessing/validation_data-00000-of-00001.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        help='Output file to write results to.')

    parser.add_argument(
        '--pid',
        dest='pid',
        help='project id')

    parser.add_argument(
        '--mbucket',
        dest='mbucket',
        help='model bucket name')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args)
    print(pipeline_args)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Test ML model with validation data
    # Output is the model performance score
    with beam.Pipeline(options=pipeline_options) as p:
        output = (p | 'CreateFileNameObject' >> beam.Create([known_args.input])
                  | 'TestMLmodel' >> beam.FlatMap(test_model, known_args.pid, known_args.mbucket)
                  )
        output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
