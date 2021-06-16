import boto3
import configparser
import json

def create_bucket(s3_client, bucket_name):
    location = {'LocationConstraint': 'us-west-2'}
    s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)


def upload_code(s3_client, file_name, bucket_name):
    s3_client.upload_file(file_name, bucket_name, 'etl.py')



def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    s3_client = boto3.client(
        's3',
        region_name='us-west-2',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY'],
    )

    create_bucket(s3_client, config['S3']['OUTPUT_BUCKET'])
    create_bucket(s3_client, config['S3']['CODE_BUCKET'])
    upload_code(s3_client, 'etl.py', config['S3']['CODE_BUCKET'])




if __name__ == '__main__':
    main()