import xml.dom.minidom as parser
import pandas as pd
import argparse
import time
from extract_processor import extract_data
from transform_processor import perform_transformation
from load_processor import load_data
import etlUtils


def perform_transform_load(transformations, destinations, data_source_map) :
    
    transformation_list = etlUtils.getChildElements(transformations)
    intermediate_data = perform_transformation(transformation_list, data_source_map)  
    load_data(destinations, data_source_map, intermediate_data)
    
arg_parser = argparse.ArgumentParser(description='Stream ETL configuration app')
arg_parser.add_argument('--conf-file', action="store", dest='conf_file', default=0)
args = arg_parser.parse_args()

rows_read = {'csv':None , 'db':None}
doc = parser.parse(args.conf_file)

while(1) : 
    time.sleep(1)
    sources = doc.getElementsByTagName('extract')[0]
    data_source_map, rows_read = extract_data(sources, rows_read)
    if len(data_source_map) : 
        transformations = doc.getElementsByTagName('transformations')[0]
        destinations = doc.getElementsByTagName('load')[0]

        perform_transform_load(transformations, destinations, data_source_map)