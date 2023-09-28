from pymongo import MongoClient
from datetime import datetime
from alive_progress import alive_bar
from typing import Tuple
import gridfs
import time
import re
import os
import sys
import string
import random
import json
from statistics import mean
import pickle

def uuid():
    """ Method to generate random uuid.
    """
    import uuid
    return str(uuid.uuid4())

def get_filename():
    """ Method to generate random filename.
    """
    return str(''.join(random.choices(string.ascii_lowercase + string.digits, k=7)))

def mongo_conn():
    """ Method to establish the connection with mongo database.
    """
    try:
        conn = MongoClient(host='127.0.0.1', port=27017)
        return conn.sample_collection
    except Exception as e:
        print("Error in mongo connection:", e)
        exit(1)

def read_the_data_from_json_file(file_path):
    """ Method to read the data from the json file.
    
    loads the json data and get the 'log' key, then returns the data.
    """
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            # regex to remove the 'Command failed with error code 2 and error:' from the starting of the log message.
            data['log'] = re.sub(r'^Command failed with error code 2 and error: ', '', data['log'])
            return data['log']
    except Exception as e:
        print('Error in reading the json file: ', e)
        exit(1)
    
def fetch_and_prepare_data_for_mongo_collection(data, timestamp):
    """ Method to fetch and prepare the data for mongo collection.
    
    Example data: 2022-06-15 18:38:06.437 8844 INFO rally.common.plugin.discover [-] Loading plugins from directories /opt/touchstone/touchstone_engine_venv/lib/python3.7/site-packages/touchstone/plugins/rally/*\u001b[00m
    1. fetch the date and time from the data and convert it into timestamp ie, 2022-06-15 18:38:06.437.
    2. fetch the process id ie, 8844.
    3. fetch the log level ie, INFO.
    4. fetch the module name ie, rally.common.plugin.discover.
    5. fetch the log message.
    And, split the data into 5 parts and return the data as a dictionary.
    """
    regex = r"([0-9]+) ([A-Z]+) ([A-Za-z0-9.]+) (.*)"
    matches = re.search(regex, data, re.DOTALL)
    data_dict = {}
    try:
        data_dict['timestamp'] = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
        data_dict['process_id'] = matches.group(1)
        data_dict['log_level'] = matches.group(2)
        data_dict['module_name'] = matches.group(3)
        data_dict['log'] = matches.group(4) if len(matches.groups()) == 4 else ''
    except Exception as e:
        print('Error in fetching the data: ', e)
        exit(1)
    # print(f'Data: {data}\nData dict: {data_dict}')
    return data_dict

def store_data_to_mongo_collection(db, data):
    """ Store every line of mongo collection as a new document.
    
    Collection Schema: uuid, timestamp, log_level and log.
    """
    try:
        db.logs.insert_one({
            'uuid': data['uuid'],
            'execution_id': data['execution_id'],
            'timestamp': data['timestamp'],         # 'timestamp': 2022-06-15 18:38:06.437
            'process_id': data['process_id'],       # 'process_id': '8844'
            'log_level': data['log_level'],         # 'log_level': 'INFO'
            'module_name': data['module_name'],     # 'module_name': 'rally.common.plugin.discover'
            'log': data['log']
        })
    except Exception as e:
        print('Error in storing data to mongo collection: ', e)
        exit(1)
        

def store_data_to_gridfs(db, fs, data, filename=get_filename(), chunksize=16777154):
    """ Method to store the data to gridfs.
    """
    print(f'Storing data to gridfs with filename: {filename!r}')
    data = data.encode('utf-8')
    try:
        fs.put(data, filename=filename, chunkSize=chunksize)
    except Exception as e:
        print('Error in storing data to gridfs: ', e)
        exit(1)

def get_data_from_gridfs(db, fs, filename, skip, page_size, count=False):
    """ Method to get the data from gridfs using filename.
    """
    if count:
        count = db.fs.chunks.count()
        return count
    try:
        print(f'Getting data from gridfs with filename: {filename!r}')
        data = db.fs.files.find({'filename': filename}).skip(skip).limit(page_size)
        content = []
        with alive_bar(len(list(data)), bar='bubbles', title='Fetch Logs from GridFS: ') as bar:
            for data_ in list(data):
                bar()
                my_id = data_['_id']
                content.append(fs.get(my_id))
        return content
    except Exception as e:
        print('Error in getting data from gridfs: ', e)
        exit(1)

def split_the_logs_by_timestamp(log_data: str):
    """ Method to split the logs by timestamp(2022-06-15 18:38:06.437) while retaining the timestamps within each log entry.
    """
    timestamp_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}'
    log_entries = re.split(timestamp_pattern, log_data)[1:]
    timestamps = re.findall(timestamp_pattern, log_data)
    return len(log_entries), zip(log_entries, timestamps)

def load_and_pickle_the_logs_from_file(file_path: str):
    """ Method to load and pickle the logs from file.

    If not pickled, pickle the data and store it in the file.
    If pickled object is present, unpickle the data and return it.
    """
    if os.path.exists('logs_from_file.pickle'):
        with open('logs_from_file.pickle', 'rb') as f:
            logs_from_file = pickle.load(f)
        print('Unpickled the data from the file.')
        return logs_from_file
    
    logs_from_file = read_the_data_from_json_file(file_path)
    with open('logs_from_file.pickle', 'wb') as f:
        pickle.dump(logs_from_file, f)
    print('Pickled the data to the file.')
    return logs_from_file

def store_data(*args, gridfs_=False, **kwargs):
    """ Main method to execute the program. 
        
    And, monitor the time taken to store the data to mongo collection.
    """
    st = time.perf_counter()
    db = mongo_conn()
    file_path = os.path.join(os.getcwd(), 'rally_report_sample.json')
    time_taken = []

    logs_from_file: str = load_and_pickle_the_logs_from_file(file_path)

    if gridfs_:
        fs = gridfs.GridFS(db)
        start_time = time.perf_counter()
        store_data_to_gridfs(db, fs, logs_from_file)
        print(f'Time taken to store data to gridfs in seconds: {time.perf_counter() - start_time:.4f}')
        print(f'Total time taken to execute the program in seconds: {time.perf_counter() - st:.4f}')
        return


    logs: Tuple[int, zip] = split_the_logs_by_timestamp(logs_from_file)
    len_logs, logs = logs
    execution_id = uuid()
    with alive_bar(len_logs*2, bar='bubbles') as bar:
        for log, timestamp in logs:
            data = fetch_and_prepare_data_for_mongo_collection(log, timestamp)
            bar()
            data['uuid'] = uuid()
            data['execution_id'] = execution_id
            start_time = time.perf_counter()
            store_data_to_mongo_collection(db, data)
            time_taken.append(time.perf_counter() - start_time)
            bar()
        else:
            print(f'Maximum time taken by slowest iteration in seconds: {max(time_taken):.4f}')
            print(f'Minimum time taken by fastest iteration in seconds: {min(time_taken):.4f}')
            print(f'Average time taken to store data to mongo collection in seconds: {mean(time_taken):.4f}')
            print(f'Total time taken to store data to mongo collection in seconds: {sum(time_taken):.4f}')
    
    print(f'Total time taken to execute the program in seconds: {time.perf_counter() - st:.4f}')



def get_data_from_mongo_collection(db, skip, page_size, count=False):
    """ Method to get the data from mongo collection after sorting by timestamp.
    """
    pipeline = [
        {"$sort": {"timestamp": 1}},  # Use 1 for ascending, -1 for descending
        ]
    with alive_bar(1, bar='bubbles', title='Fetch Logs from Collection: ') as bar:
        if count:
            try:
                pipeline.append(
                     {
                        "$group": {
                            "_id": None,
                            "count": {"$sum": 1}
                        }
                    }
                )
                count = list(db.logs.aggregate(pipeline, allowDiskUse=True))[0]['count']
                bar()
            except Exception as e:
                print('Error in fetching the count of documents from mongo collection: ', e)
                exit(1)
            return count
        pipeline.extend([
            {"$skip": skip},
            {"$limit": page_size}
            ])
        try:
            # content = db.logs.find().skip(skip).limit(page_size)
            content = list(db.logs.aggregate(pipeline, allowDiskUse=True))
            bar()
            # return db.logs.find().sort('timestamp').allowDiskUse(True)
        except Exception as e:
            print('Error in fetching the data from mongo collection: ', e)
            exit(1)
    return content

def prepare_logs_from_mongo_collection(data):
    """ Method to prepare the logs from mongo collection.
    """
    content_ = []
    with alive_bar(len(data), bar='bubbles', title='Prepare Data: ') as bar:
        try:
            for d in data:
                log_line = f"{d['timestamp']} {d['process_id']} {d['log_level']} {d['module_name']} {d['log']}"
                content_.append(log_line)
                bar()
        except Exception as e:
            print('Error in preparing the logs from mongo collection: ', e)
            exit(1)
    return content_

def write_logs_to_file(logs, splitted=True):
    """ Method to write the logs to file.
    """
    try:
        with open('logs_from_mongo_collection.txt', 'w') as f:
            if splitted:
                f.write('\n'.join(logs))
            else:
                f.write(logs)
    except Exception as e:
        print('Error in writing the logs to file: ', e)
        exit(1)

def get_data(page_number, page_size, gridfs_=False, count=False):
    """ Main method to execute the program. 
        
    And, monitor the time taken to get the data from mongo collection.
    """
    st = time.perf_counter()
    db = mongo_conn()

    # Calculate the skip value to skip documents for pagination
    skip = (page_number - 1) * page_size

    # documents = collection.find().skip(skip).limit(page_size)
    if gridfs_:
        filename = sys.argv[1]
        if not filename:
            print('Please provide the filename to get the data from gridfs.')
            exit(1)
        fs = gridfs.GridFS(db)
        start_time = time.perf_counter()
        data = get_data_from_gridfs(db, fs, filename, skip, page_size, count)
        if count: 
            print(data)
            return
        print(f'Time taken to get data from gridfs in seconds: {time.perf_counter() - start_time:.4f}')
        logs = []
        for log in data:
            logs.append(log.read().decode('utf-8'))
        logs = ''.join(logs)
        write_logs_to_file(logs, splitted=False)
        print(f'Total time taken to execute the program in seconds: {time.perf_counter() - st:.4f}')
        return
    start_time= time.perf_counter()
    if count:
        count_ = get_data_from_mongo_collection(db, skip, page_size, count)
        print(f'Total no of pages: {(count_+page_size-1)//page_size}, Documents count: {count_}')
        print(f'Time taken to get the count of documents from mongo collection in seconds: {time.perf_counter() - start_time:.4f}')
        print(f'Total time taken to execute the program in seconds: {time.perf_counter() - st:.4f}')
        return
    data : list[dict] = get_data_from_mongo_collection(db, skip, page_size)
    print(f'Page Number: {page_number}, Page size: {page_size}, Time: {time.perf_counter() - start_time:.4f}')
    # print(f'Time taken to get data from mongo collection in seconds: {time.perf_counter() - start_time:.4f}')
    logs = prepare_logs_from_mongo_collection(data)
    write_logs_to_file(logs)
    print(f'Total time taken to execute the program in seconds: {time.perf_counter() - st:.4f}')

def main(func):
    gridfs_ = True
    if gridfs_ and func == get_data:
        filename = sys.argv[1]
    else:
        page_number = int(sys.argv[1]) if len(sys.argv) > 1 else 1
        page_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        count = True if len(sys.argv) > 3 else False
    func(page_number, page_size, gridfs_, count)

if __name__ == '__main__':
    # main(get_data)
    main(store_data)
    # store_data(gridfs_=False)
    # get_data(page_number=1, page_size=10, gridfs_=False, count=False)
    





















# def main(db, fs, chunksize, data, count):

#     def get_filename():
#         return str(''.join(random.choices(string.ascii_lowercase + string.digits, k=7)))

#     start_time = time.time()
    
#     upload_start_time = time.time()
#     filenames = []
#     ls = []
#     for i in range(count):
#         lst = []
#         for d in data:
#             a = time.time()
#             file_name = get_filename()
#             fs.put(d, filename = file_name, chunkSize = chunksize)
#             lst.append(time.time() - a)
#             filenames.append(file_name)
#         else:
#             ls.append(mean(lst))
#     print('Upload mean time: ', mean(ls))
#     print("upload time taken: ", time.time() - upload_start_time)

#     download_start_time = time.time()
#     ls = []
#     for file_name in filenames:
#         a = time.time()
#         file_data= db.fs.files.find_one({'filename': file_name})
#         my_id = file_data['_id']
#         outputdata = fs.get(my_id).read()
#         ls.append(time.time() - a)
#     print('Download mean time: ', mean(ls))
    

#     # if data != outputdata:
#     #     print('ERROR!!!')
#     #     exit(1)

#     print("Download time taken: ", time.time() - download_start_time)
#     print('Total time taken by iteration: ', time.time() - start_time)
#     print()

# fs = gridfs.GridFS(db)
# # print(type(db))
# # chunksize = int(sys.argv[1]) * 1024 * 1024 if len(sys.argv) > 1 else 255 * 1000
# chunksize = 16777154
# data = (
#         *(b'test data, \n' * 2000000,) * 9,      # ~ 200 Mb
#         # *(b'test data, \n' * 5000,) * 35,        # ~ 5 Kb
#         # *(b'test data, \n' * 2500000,) * 14,      # ~ 25 Mb
#     )      

# count = 10

# print('Chunk Size in bytes: ', chunksize)
# print(f'Total no of gridfs instances: {count}\n')

# for i in range(5):
#     print('Iteration: ',i+1)
#     try:
#         main(db, fs, chunksize, data, count)
#         print('Storing data to mongo collection...')
#         t1 = time.time()
#         for d in data:
#             for d1 in d.splitlines():
#                 store_data_to_mongo_collection(db, {
#                     'uuid': uuid(),
#                     'timestamp': time.time(),
#                     'log_level': 'INFO',
#                     'log': d1
#                 })
#         print('Time taken to store data to mongo collection: ', time.time() - t1)
#     except Exception as e:
#         print('Error!!! ',e)
#         exit(1)
