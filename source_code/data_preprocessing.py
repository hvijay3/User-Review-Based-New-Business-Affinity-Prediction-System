import argparse
import csv
import json
import sys

def read_json_write_csv(json_file_name, csv_file_name, column_names):
    
    with open(json_file_name) as json_file, open(csv_file_name, 'wb') as csv_file:
        csv_writer = csv.writer(csv_file)
        write_header = True
        
        count = 0
        for entry in json_file:
            count = count + 1
            print(count)
            #if count == 1000:
                #break
            py_entry_dict = json.loads(entry)
            if write_header:
                csv_writer.writerow(list(column_names))
                write_header = False
            csv_writer.writerow(csv_row(py_entry_dict, column_names))


def csv_row(py_entry_dict, column_names):
    
    row = []
    
    for col in column_names:
        
        value = get_key_value(py_entry_dict, col)
        
        if value is not None:
            row.append(to_string(value))
        
        else:
            row.append('')

        print(row)
    return row


def get_key_value(dict, key):

    if '.' not in key:
        if key not in dict:
            return None
        return dict[key]
    
    base_key, nested_key = key.split('.', 1)
    
    if base_key not in dict:
        return None
    
    return get_key_value(dict[base_key], nested_key)


def get_all_column_names_from_file(json_file_path):

    column_names = set()
    
    with open(json_file_path, 'r') as json_file:
        
        count = 0
        for entry in json_file:
            count = count + 1
            print(count)
            #if count == 1000:
                #break
            #loads the json object as python dict"""
            py_entry_dict = json.loads(entry)
            #flatten the nested json python dict to obtain all the column names in each entry
            for key, val in py_entry_dict.items():
                column_names.update(set(flatten_col(key, val).keys()))
    
    return column_names


def to_string(s):
    try:
        return str(s)
    except:
        #Change the encoding type if needed
        return s.encode('utf-8')

column_list = []
def flatten_col(key, val):
    
    global column_list 
    # based on the type of val, form the nested column names 
    
    if type(val) is dict:
        sub_keys = val.keys()      
        for sub_key in sub_keys:
            flatten_col(to_string(key) + '.' + to_string(sub_key), val[sub_key])     
 
    else:
        column_list.append((to_string(key), val))
        
   
    return dict(column_list)

    
if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='JSON to CSV Conversion of Yelp DataSet',
    )

    parser.add_argument(
        'json_file',
        type=str,
        help='Input json file',
    )

    args = parser.parse_args()
    print(args)
    json_file = args.json_file
    
    csv_file = '{name}.csv'.format(name=json_file.split('.json')[0])
    
    print(json_file)
    print(csv_file)

    #json_file = "C:\\Fall\'17\\IDS\\Project\\UserReviewBasedNewBusinessAffinityPredictionSystem\\DataPreprocessing\\review.json"
    #csv_file = "C:\\Fall\'17\\IDS\\Project\\UserReviewBasedNewBusinessAffinityPredictionSystem\\DataPreprocessing\\review.csv"
    
    column_names = get_all_column_names_from_file(json_file)
    
    #print(column_names)
    read_json_write_csv(json_file, csv_file, column_names)