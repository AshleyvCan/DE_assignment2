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

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'


def convert_to_df(gs_data):
    dict_csv = csv.DictReader(gs_data)

    # Create the DataFrame
    df = pd.DataFrame(dict_csv)

    # Remove index and convert data to numeric
    df = df.iloc[:, 1:]
    print(df.columns)
    headers = ['Setting_0', 'Setting_1', 'Setting_2', 'Sensor_0', 'Sensor_1', 'Sensor_2', 'Sensor_3', 'Sensor_4',
               'Sensor_5','Sensor_6', 'Sensor_7', 'Sensor_8', 'Sensor_9', 'Sensor_10', 'Sensor_11', 'Sensor_12',
               'Sensor_13','Sensor_14','Sensor_15', 'Sensor_16', 'Sensor_17', 'Sensor_18', 'Sensor_19', 'Sensor_20',
               'RUL']
    df = df.apply(pd.to_numeric)

    df.columns = headers
    print(df.head())
    return df

# Remove features with low or no variance
def remove_novariance(joblib_name, project_id, bucket_name, gs_data, threshold = 0):
    df = convert_to_df(gs_data)

    # Perform feature selection only on the independent variables
    X = df.loc[:, df.columns != 'RUL']

    # Fit the feature selection method
    variance_selector = feature_selection.VarianceThreshold(threshold= threshold)
    variance_selector.fit(X)

    # Save the selector in bucket
    save_model(variance_selector, project_id, bucket_name, joblib_name)#'variance_selector.joblib')

    # Apply selector on training data
    columns_variance = variance_selector.get_support()
    X = pd.DataFrame(variance_selector.transform(X), columns = X.columns.values[columns_variance])

    df = pd.concat([X, df['RUL']], axis =1).to_csv()
    yield df

# Save model in bucket
def save_model(model, project_id, bucket_name, model_file):
    joblib.dump(model,model_file)

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob('preproces_models/' + model_file)
    blob.upload_from_filename(model_file)
    logging.info(model_file + "is saved in a GCP bucket")

# Split the data in train and validation set
# The first 20000 rows are in de training set
def splitting(row,n_partitions):
    print(row)
    if row.split(',')[0] == '' or int(row.split(',')[0]) < 20000:
        return 0
    return 1

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default = 'gs://de2020assignment2/data/train_set.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output1',
        dest='output1',
        default = 'gs://de2020assignment2/preprocessing/train_data',
        help='Output file to write results to.')
    parser.add_argument(
        '--output2',
        dest='output2',
        default='gs://de2020assignment2/preprocessing/validation_data',
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

        # Split data in train and validation set
        train_set, validation_set = (p | 'Create FileName Object' >> beam.io.ReadFromText(known_args.input)
                  | 'SplitTrainAndValidation' >> beam.Partition(splitting,2))


        # Train feature selector only on training data
        train_set = (p | 'SpecifyPreprocessing' >> beam.Create(['variance_selector.joblib'])
                   |'RemoveFeatureNoVariance' >> beam.ParDo(remove_novariance, known_args.pid, known_args.mbucket, gs_data = beam.pvalue.AsList(train_set)))

        # Write datasets to GDS
        train_set | 'WriteTrain' >> WriteToText(known_args.output1, file_name_suffix=".csv")
        validation_set | 'WriteValidation' >> WriteToText(known_args.output2,
                                                          file_name_suffix=".csv")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
