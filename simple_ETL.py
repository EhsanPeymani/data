import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as et
import glob

tmpfile     = 'etl/temp.tmp'
logfile     = 'ETL/log.txt'
targetfile  = 'ETL/transformed_data.csv'

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df


def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df


def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns=['name', 'height', 'weight'])
    tree = et.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        person_dict = {'name':name, 'height':height, 'weight':weight}
        person_df = pd.DataFrame(person_dict, index=[0])
        df = pd.concat([df, person_df], ignore_index = True)
    return df


def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])
    
    all_csv_files = glob.glob("source/*.csv")
    for file in all_csv_files:
        df = extract_from_csv(file)
        extracted_data = pd.concat([extracted_data, df], ignore_index = True)
        
    all_json_files = glob.glob("source/*.json")
    for file in all_json_files:
        df = extract_from_json(file)
        extracted_data = pd.concat([extracted_data, df], ignore_index = True)
        
    all_xml_files = glob.glob("source/*.xml")
    for file in all_xml_files:
        df = extract_from_xml(file)
        extracted_data = pd.concat([extracted_data, df], ignore_index = True)
    
    return extracted_data
    
    
def transform(data):
    data.height = data.height.astype(float)
    data['height'] = round(data.height * 25.4, 2)
    
    data.weight = data.weight.astype(float)
    data['weight'] = round(data.weight * 0.453592, 2)
    return data


def load(target_file, data):
    data.to_csv(target_file)
    
    
def log(message, log_file):
    timestamp_format = '%Y-%h-%d-%H:%M:%S.%f'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    message = timestamp + ", " + message + '\n'
    print(message)
    with open(log_file, 'a') as file:
        file.write(message)
    

log("Extracting data", logfile)
extracted_data = extract()
log("Data extracted", logfile)

log("Transforming data", logfile)
transformed_data = transform(extracted_data)
log("Data Transformed", logfile)

log("Loading data into CSV file", logfile)
load(target_file=targetfile, data=transformed_data)
log("Data Loaded", logfile)



