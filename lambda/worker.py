import pickle
import boto3
import json
import time


def apply_function(f, args, kwargs, time_delay):
    start = time.time()
    try:
        result = f(*args, **kwargs)
    except Exception as e:
        msg = {'op': 'task-erred', 'actual-exception': e}
    else:
        msg = {'op': 'task-finished',
               'status': 'OK',
               'result': result,
               'nbytes': 0,
               'type': str(type(result)) if result is not None else None}  # Super hacky, redo
    finally:
        end = time.time()
    msg['start'] = start + time_delay
    msg['stop'] = end + time_delay
    return msg


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket = event['s3_bucket']
    key = event['s3_key']

    print 'reading from s3'
    msg = json.loads(s3.Object(bucket, key).get()['Body'].read())

    print 'unpickling function'
    f = pickle.loads(''.join([chr(c) for c in msg['function']]))

    print 'unpickling args'
    args = pickle.loads(''.join([chr(c) for c in msg['args']]))

    print 'running function'
    result = apply_function(f, args, **event['kwargs'])

    print 'writing to s3'
    output_key = 'result' + key
    s3.Bucket(bucket).put_object(Key=output_key, Body=pickle.dumps(result['result']))

    print 'updating results'
    result['result'] = {'s3_bucket': bucket, 's3_key': output_key}

    print 'results: {}'.format(result)
    return result

