#!/usr/bin/env python3

import argparse
import boto3
import json
import os
import subprocess

def list_s3_prefixes(bucket, root_prefix):
    """List all subdirectories (prefixes) within the specified root prefix."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=root_prefix, Delimiter='/'):
        for common_prefix in page.get('CommonPrefixes', []):
            yield common_prefix['Prefix']

def process_s3_prefixes(bucket, root_prefix, processed_file):
    """Process each subdirectory and call the checksum script."""
    # Load the list of processed prefixes if it exists
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed_prefixes = set(json.load(f))
    else:
        processed_prefixes = set()

    for prefix in list_s3_prefixes(bucket, root_prefix):
        # Skip already processed prefixes
        if prefix in processed_prefixes:
            print(f"Skipping already processed prefix: {prefix}")
            continue
        
        # Call the checksum script
        cmd = [
            'python3', 'checksums.py',  # Replace with your actual script name
            '--bucketName', bucket,
            '--prefix', prefix
        ]
        
        print(f"Processing prefix: {prefix}")
        
        try:
            result = subprocess.run(cmd, check=True)
            # Record the prefix as processed
            processed_prefixes.add(prefix)
            with open(processed_file, 'w') as f:
                json.dump(list(processed_prefixes), f)
        except subprocess.CalledProcessError as e:
            print(f"Error processing prefix {prefix}: {e}")
            # Optionally, handle retries or log error details here

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process S3 prefixes and call checksum script for each.')
    parser.add_argument('--bucketName', required=True, help='Name of the S3 bucket')
    parser.add_argument('--rootPrefix', required=True, help='Root prefix (directory) in the S3 bucket')
    parser.add_argument('--processedFile', default='processed_prefixes.json', help='File to track processed prefixes')
    
    args = parser.parse_args()
    
    process_s3_prefixes(args.bucketName, args.rootPrefix, args.processedFile)
