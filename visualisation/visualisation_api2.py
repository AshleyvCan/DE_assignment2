from __future__ import absolute_import
#from flask import Flask, request, make_response, Response, json
import os
#from matplotlib.backends.backend_agg import FigureCanvasAgg
import io
#from resources.plotting import plot_pred

import argparse
import csv
import io
import json
import logging

import grpc
import apache_beam as beam
import pandas as pd
from apache_beam.io import Read, BigQuerySource
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from sklearn import neighbors, metrics
from google.cloud import storage
import joblib

# https://stackoverflow.com/questions/50728328/python-how-to-show-matplotlib-in-flask
# https://gist.github.com/rduplain/1641344
#app = Flask(__name__)

import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/20200191/Documents/data_engineering/DE2020/lab8/de2020-6-6a00f5d73faa.json'

class ParseActivityEventFn(beam.DoFn):

    def __init__(self):
        beam.DoFn.__init__(self)
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            print(int(elem['total_score']))
        except:  # pylint: disable=bare-except
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem)

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        help='Input file to process.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    print(known_args)
    print(pipeline_args)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:

        output = (p | 'SQLTable' >> Read(BigQuerySource(query ='SELECT total_score FROM '\
                                                        '`de2020-6.gamedata.jads_users` LIMIT 1000',
                                                   use_standard_sql= True))
        #print(output)
                  #| 'DecodeString' >> beam.Map(lambda b: b.decode('utf-8'))
                  | 'ParseActivityEventFn' >> beam.ParDo(ParseActivityEventFn()))

   #output | 'Write' >> WriteToText('gs://')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
#app.config["DEBUG"] = True
#app.run(host='0.0.0.0', port=5000)