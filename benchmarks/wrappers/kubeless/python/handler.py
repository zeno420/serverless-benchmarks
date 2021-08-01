import datetime, io, json, os, sys, uuid
from pprint import pprint


#TODO: envrions in aus request ziehen

def handler(event, context):

    income_timestamp = datetime.datetime.now().timestamp()

    if 'data' in event:
        data = event['data']
    else:
        data = {}
    data['request-id'] = event['event-id']
    data['income-timestamp'] = income_timestamp

    #prepare environment with storage vars
    os.environ['MINIO_ADDRESS'] = data['minio_sebs_storage_url']
    os.environ['MINIO_ACCESS_KEY']= data['minio_sebs_storage_access_key']
    os.environ['MINIO_SECRET_KEY']= data['minio_sebs_storage_secret_key']

    begin = datetime.datetime.now()

    from function import function
    ret = function.handler(data)
    end = datetime.datetime.now()

    log_data = {
        'output': ret['result']
    }

    if 'measurement' in ret:
        log_data['measurement'] = ret['measurement']
    if 'logs' in data:
        log_data['time'] = (end - begin) / datetime.timedelta(microseconds=1)
        results_begin = datetime.datetime.now()
        from function import storage
        storage_inst = storage.storage.get_instance()
        b = data.get('logs').get('bucket')
        storage_inst.upload_stream(b, '{}.json'.format(event['event-id']),
                io.BytesIO(json.dumps(log_data).encode('utf-8')))
        results_end = datetime.datetime.now()
        results_time = (results_end - results_begin) / datetime.timedelta(microseconds=1)
    else:
        results_time = 0

    # cold test
    is_cold = False
    fname = os.path.join('/tmp', 'cold_run')
    if not os.path.exists(fname):
        is_cold = True
        container_id = str(uuid.uuid4())[0:8]
        with open(fname, 'a') as f:
            f.write(container_id)
    else:
        with open(fname, 'r') as f:
            container_id = f.read()

    cold_start_var = ""
    if "cold_start" in os.environ:
        cold_start_var = os.environ["cold_start"]

    return json.dumps({
            'begin': begin.strftime('%s.%f'),
            'end': end.strftime('%s.%f'),
            'results_time': results_time,
            'is_cold': is_cold,
            'result': log_data,
            'request_id': event['event-id'],
            'cold_start_var': cold_start_var,
            'container_id': container_id,
        })
