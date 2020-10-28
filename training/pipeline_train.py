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


# Train and Save ML model
def train_model(gs_data, project_id, bucket_name):

    # Read the csv file
    gs_data = beam.io.filesystems.FileSystems.open(gs_data)
    df = pd.read_csv(io.TextIOWrapper(gs_data), index_col = 0)

    X = df.loc[:, df.columns != 'RUL']
    Y = df['RUL']

    # Training of ML model
    knn_model = neighbors.KNeighborsClassifier(n_neighbors=2, weights='uniform')
    knn_model.fit(X, Y)

    # Save model in bucket
    save_model(knn_model, project_id, bucket_name)
    print(X.head())

    # Evaluate model performance on training data
    MAE_score = metrics.mean_absolute_error(Y, knn_model.predict(X))
    print(MAE_score)
    return json.dumps('MAE_score: ', sort_keys=False, indent=4)

# Save model in bucket
def save_model(model, project_id, bucket_name):
    joblib.dump(model,"model.joblib")

    client = storage.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob('ml_models/model.joblib')
    blob.upload_from_filename('model.joblib')
    logging.info("The ML model is saved in a GCP bucket")

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://de2020labs7/data/preprocess_data-00000-of-00001.csv',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        default='gs://YOUR_OUTPUT_BUCKET/AND_OUTPUT_PREFIX',
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

    # Train and save ML model with training data
    # Output is the model performance score
    with beam.Pipeline(options=pipeline_options) as p:
        output = (p | 'CreateFileNameObject' >> beam.Create([known_args.input])
                  | 'TrainAndSaveMLmodel' >> beam.FlatMap(train_model, known_args.pid, known_args.mbucket)
                  )
        output | 'Write' >> WriteToText(known_args.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
