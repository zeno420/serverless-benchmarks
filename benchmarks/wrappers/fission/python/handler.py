import datetime, io, json, os, sys, uuid
from posixpath import join
from pprint import pprint
from flask import request, current_app
import traceback


#TODO: envrions in aus request ziehen

def handler():
    try:

        income_timestamp = datetime.datetime.now().timestamp()

        data = request.get_json(force=True)
        headers = request.headers

        current_app.logger.info("1")

        data['request-id'] = headers['X-B3-Traceid'] if 'X-B3-Traceid' in headers else ''
        data['income-timestamp'] = income_timestamp

        current_app.logger.info("2")

        #prepare environment with storage vars
        os.environ['MINIO_ADDRESS'] = data['minio_sebs_storage_url']
        os.environ['MINIO_ACCESS_KEY']= data['minio_sebs_storage_access_key']
        os.environ['MINIO_SECRET_KEY']= data['minio_sebs_storage_secret_key']

        begin = datetime.datetime.now()

        current_app.logger.info("3")


        from function import function

        current_app.logger.info("4")

        ret = function.handler(data)
        end = datetime.datetime.now()

        current_app.logger.info("5")

        log_data = {
            'output': ret['result']
        }

        current_app.logger.info("6")

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

        current_app.logger.info("7")

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

        current_app.logger.info("8")

        cold_start_var = ""
        if "cold_start" in os.environ:
            cold_start_var = os.environ["cold_start"]

        return json.dumps({
                'begin': begin.strftime('%s.%f'),
                'end': end.strftime('%s.%f'),
                'results_time': results_time,
                'is_cold': is_cold,
                'result': log_data,
                'request_id': data['request-id'],
                'cold_start_var': cold_start_var,
                'container_id': container_id,
            })
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        current_app.logger.info("exceptioöööön")
        return '\n'.join(traceback.format_exception(exc_type, exc_value, exc_traceback)), 500
