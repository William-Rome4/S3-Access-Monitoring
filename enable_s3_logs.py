import boto3

def create_folder(source, path, client):
    client = boto3.client('s3')
    if not path.endswith('/'):
        path += '/'
    client.put_object(Bucket=source, Key=path)

def enable_logs(root, prefix, target, client):
    client = boto3.client('s3')
    logging = {
        'LoggingEnabled': {
            'TargetBucket': target,
            'TargetPrefix': f"{prefix}/{bucket_name}/"
        }
    }
    client.put_bucket_logging(
        Bucket=root,
        BucketLoggingStatus=logging
    )

if __name__ == "__main__":
    bucket_name = ""
    logging_bucket = ""
    prefix = ""
    s3_client = boto3.client('s3')

    create_folder(logging_bucket, f"{prefix}/{bucket_name}", s3_client)
    enable_logs(f"{bucket_name}", prefix, logging_bucket, s3_client)
    
