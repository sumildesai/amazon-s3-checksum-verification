import os
import subprocess
import argparse
import logging
from datetime import datetime

# Set up logging
log_filename = f"s3verify_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
logging.basicConfig(filename="./logs/" + log_filename, level=logging.WARNING, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Argument parsing
parser = argparse.ArgumentParser(description='Validate S3 objects with local files using s3verify')
parser.add_argument('--bucketName', required=True, help='Name of the S3 bucket')
parser.add_argument('--localDir', required=True, help='Path to the local directory')
parser.add_argument('--s3Prefix', required=False, help='S3 prefix to match local files against')

args = parser.parse_args()

def traverse_and_verify(local_dir, s3_prefix, bucket_name):
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            local_path = os.path.join(root, file)
            # Construct the corresponding S3 key
            relative_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
            s3_key = f"{s3_prefix}/{relative_path}" if s3_prefix else relative_path
            s3_uri = f"s3://{bucket_name}/{s3_key}"
            logging.info(f"Verifying: {local_path} against {s3_uri}")
            
            # Execute the s3verify command
            result = subprocess.run(["./s3verify", local_path, s3_uri],
                                    capture_output=True, text=True)
                      
            # Log the s3verify output under the S3 key
            if result.returncode == 0:
                logging.info(f"PASS: {s3_key}\n {result.stderr.strip()}")
            else:
                logging.warning(f"FAIL: {s3_key}\n {result.stdout.strip()}\n {result.stderr.strip()}")

if __name__ == '__main__':
    traverse_and_verify(args.localDir, args.s3Prefix, args.bucketName)
