#!/usr/bin/env python3

import boto3
import botocore
import csv
import argparse
import sys
from datetime import datetime

parser = argparse.ArgumentParser(description='Retrieve and output checksums for S3 objects to a CSV file')
parser.add_argument('--bucketName', required=True,
                    help='Name of the S3 bucket storing the objects')
parser.add_argument('--prefix', 
                    help='Prefix for the S3 objects to retrieve checksums from', default='')
args = parser.parse_args()

def list_s3_objects(bucket, prefix=''):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key']

def s3checksumResult(objectSummary):

    checksumAlgo = whichChecksum(objectSummary)

    return objectSummary['Checksum'][checksumAlgo]

def whichChecksum(objectSummary):

    try:
        checksumHashes = objectSummary['Checksum']
        for checksum in checksumHashes:

            return checksum
    except KeyError:
        print("\nChecksum is not enabled on the object. Please add checksums using the copy-object operation before validating checksums. See this documentation for more details, https://aws.amazon.com/blogs/aws/new-additional-checksum-algorithms-for-amazon-s3/\n")
        sys.exit(1)

def getObjectAttributes(key):

    try:
        s3 = boto3.client('s3')

        objectSummary = s3.get_object_attributes(Bucket=args.bucketName,Key=key,
            ObjectAttributes=[ 'Checksum','ObjectParts'
            ])

        return objectSummary
    except ( botocore.exceptions.ClientError, botocore.exceptions.PartialCredentialsError ):
        print("\nYou must authenticate with credentials that are allowed to read objects in the bucket the data you wish to validate is stored in.\n")
        sys.exit(1)

def generate_csv_filename(bucket, prefix):
    s3_slug = (prefix.replace('/', '_') if prefix else 'all_objects').replace('s3://', '')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{bucket}_{s3_slug}_{timestamp}.csv"

def main():
    bucket = args.bucketName
    prefix = args.prefix
    csv_file = generate_csv_filename(bucket, prefix)
    
    with open(csv_file, 'w', newline='') as csvfile:
        fieldnames = ['ObjectKey', 'Checksum']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for key in list_s3_objects(bucket, prefix):
            objectSummary = getObjectAttributes(key)
            checksum = s3checksumResult(objectSummary)
            writer.writerow({'ObjectKey': key, 'Checksum': checksum})
    
    print(f"Checksums written to {csv_file}")

if __name__ == '__main__':
    main()