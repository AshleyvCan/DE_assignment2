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
from apache_beam.dataframe import convert
from sklearn import feature_selection
from google.cloud import storage
import joblib


def convert_df(gs_data):
    gs_data = beam.io.filesystems.FileSystems.open(gs_data)
    df = pd.read_csv(io.TextIOWrapper(gs_data), index_col = 0)
    print(df.head())
    return df

# Remove features with low or no variance
def remove_novariance(gs_data, project_id, bucket_name, threshold = 0):
    df = convert_df(gs_data)
    X = df.loc[:, df.columns != 'RUL']

    # Fit the feature selection method
    variance_selector = feature_selection.VarianceThreshold(threshold= threshold)
    variance_selector.fit(X)

    # Save the selector in bucket
    save_model(variance_selector, project_id, bucket_name, 'variance_selector.joblib')

    # Apply selector on training data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns = X.columns.values[columns_variance])

    df = pd.concat([X, df['RUL']], axis =1).to_csv()
    yield df #convert.to_pcollection(df)

# Save model in bucket
def save_model(model, project_id, bucket_name, model_file):
    joblib.dump(model,model_file)

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob('preproces_models/' + model_file)
    blob.upload_from_filename(model_file)
    logging.info(model_file + "is saved in a GCP bucket")

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://de2020labs7/preprocessing/train_data',
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

    # Train and save feature selection models and apply these pre-processing steps on train data
    with beam.Pipeline(options=pipeline_options) as p:

        output = (p | 'Create FileName Object' >> beam.Create([known_args.input])
                 | 'RemoveFeatureNoVariance' >> beam.ParDo(remove_novariance, known_args.pid, known_args.mbucket))

        output | 'Write' >> WriteToText(known_args.output, file_name_suffix=".csv")
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
