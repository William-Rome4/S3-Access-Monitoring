import boto3
import time

def create_table(log, target, prefix):
    glue = boto3.client('glue')
    print(f"[ATHENA] Creating table for bucket {target}")
    try:
        glue.create_table(
            DatabaseName=log,
            TableInput={
                'Name': f"{target}_logs",
                'StorageDescriptor': {
                    'Columns': [
                        {
                            'Name': 'bucketowner',
                            'Type': 'string'
                        },
                        {
                            'Name': 'bucket_name',
                            'Type': 'string'
                        },
                        {
                            'Name': 'requestdatetime',
                            'Type': 'string'
                        },
                        {
                            'Name': 'remoteip',
                            'Type': 'string'
                        },
                        {
                            'Name': 'requester',
                            'Type': 'string'
                        },
                        {
                            'Name': 'requestid',
                            'Type': 'string'
                        },
                        {
                            'Name': 'operation',
                            'Type': 'string'
                        },
                        {
                            'Name': 'key',
                            'Type': 'string'
                        },
                        {
                            'Name': 'request_uri',
                            'Type': 'string'
                        },
                        {
                            'Name': 'httpstatus',
                            'Type': 'string'
                        },
                        {
                            'Name': 'errorcode',
                            'Type': 'string'
                        },
                        {
                            'Name': 'bytessent',
                            'Type': 'bigint'
                        },
                        {
                            'Name': 'objectsize',
                            'Type': 'bigint'
                        },
                        {
                            'Name': 'totaltime',
                            'Type': 'string'
                        },
                        {
                            'Name': 'turnaroundtime',
                            'Type': 'string'
                        },
                        {
                            'Name': 'referrer',
                            'Type': 'string'
                        },
                        {
                            'Name': 'useragent',
                            'Type': 'string'
                        },
                        {
                            'Name': 'versionid',
                            'Type': 'string'
                        },
                        {
                            'Name': 'hostid',
                            'Type': 'string'
                        },
                        {
                            'Name': 'sigv',
                            'Type': 'string'
                        },
                        {
                            'Name': 'ciphersuite',
                            'Type': 'string'
                        },
                        {
                            'Name': 'authtype',
                            'Type': 'string'
                        },
                        {
                            'Name': 'endpoint',
                            'Type': 'string'
                        },
                        {
                            'Name': 'tlsversion',
                            'Type': 'string'
                        }
                    ],
                    'Location': f"s3://{log}/{prefix}/{target}",
                    'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.serde2.RegexSerDe',
                        'Parameters': {
                            'input.regex': '([^ ]*) ([^ ]*) \\[(.*?)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*)(?: ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*))?.*$',
                        }
                    }
                }
            }
        )
    except Exception as e:
        print(f"[FAIL] {e}")

def create_folder(source, path, client):
    if not path.endswith('/'):
        path += '/'
    try:
        client.put_object(Bucket=source, Key=path)
    except Exception as e:
        print(f"[FAIL] {e}")

def create_cwlogs(log_bucket, bucket_names):
    client = boto3.client('logs')
    try:
        print("[CLOUDWATCH] Creating default Log Group")
        client.create_log_group(
            logGroupName=f"/aws/s3/{log_bucket}",
            tags={ 'Name': f"/aws/s3/{log_bucket}" }
        )
        print("[CLOUDWATCH] Creating raw data Log Group")
        client.create_log_group(
            logGroupName=f"/aws/s3/{log_bucket}-raw",
            tags={ 'Name': f"/aws/s3/{log_bucket}-raw" }
        )
    except Exception as e:
        print(f"[FAIL] {e}")

    for lg in [f"{log_bucket}",f"{log_bucket}-raw"]:
        print(f"[CLOUDWATCH] Creating Log Streams for group {lg}")
        request_count = 0
        for b in bucket_names:
            try:
                b = b.decode('utf-8').strip()
                client.create_log_stream(
                    logGroupName=f"/aws/s3/{lg}",
                    logStreamName=b
                )
            except Exception as e:
                print(f"[ERROR] Failed to create Stream for bucket {b}: {e}")
            request_count+=1
            if request_count >= 10:
                time.sleep(5)

def enable_logs(root, prefix, target, client):
    logging = {
        'LoggingEnabled': {
            'TargetBucket': target,
            'TargetPrefix': f"{prefix}/{bucket_name}/"
        }
    }
    print(f"[S3] Enabling Access Logs for bucket: {bucket_name}"
    try:
          client.put_bucket_logging(
              Bucket=root,
              BucketLoggingStatus=logging
          )
    except Exception as e:
          print(f"[FAIL] {e}")

if __name__ == "__main__":
    #Insert the name of the bucket where the logs will go
    logging_bucket = ""
    #Path to the file inside your root S3, containing all the buckets that will be monitored
    file_path = ""
    #The path to the root folder of the logs
    prefix = ""
    #Turn this value to 'True' if you already have log groups/streams created
    hasLogs = False

    s3_client = boto3.client('s3')
    
    bucket_names = s3_client.get_object(
        Bucket=logging_bucket,
        Key=file_path
    )
    bucket_names = bucket_names['Body'].readlines()

    if hasLogs == False:
        create_cwlogs(logging_bucket, bucket_names)    
    for b in bucket_names:
        bucket_name = b.decode('utf-8').strip()
        create_folder(logging_bucket, f"{prefix}/{bucket_name}", s3_client)
        enable_logs(bucket_name, prefix, logging_bucket, s3_client)
        create_table(logging_bucket, bucket_name, prefix)
