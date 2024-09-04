#!/usr/bin/env python3

import os
import boto3
import botocore
import base64
import hashlib
import argparse
import crc32c
import zlib
import sys
import logging
from datetime import datetime

parser = argparse.ArgumentParser(description='Options for integrity validation')
parser.add_argument('--bucketName', required=True,
                    help='Name of the S3 bucket storing the objects')
parser.add_argument('--localDir', required=True,
                    help='Path to the local directory to validate against S3 objects')

# Generate a timestamped log file name
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file_name = f"checksum_verification_{timestamp}.log"

# Add log file argument with default value
parser.add_argument('--logFile', default=log_file_name,
                    help='Path to the log file where output will be stored')

args = parser.parse_args()

# Configure logging
logging.basicConfig(filename=args.logFile, 
                    filemode='a', 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

def whichChecksum(objectSummary):
    try:
        checksumHashes = objectSummary['Checksum']
        for checksum in checksumHashes:
            return checksum
    except KeyError:
        logging.error("Checksum is not enabled on the object. Please add checksums using the copy-object operation before validating checksums.")
        sys.exit(1)

def getObjectAttributes(s3_client, bucket_name, s3_key):
    try:
        objectSummary = s3_client.get_object_attributes(Bucket=bucket_name, Key=s3_key,
                                                        ObjectAttributes=['Checksum', 'ObjectParts'])
        return objectSummary
    except (botocore.exceptions.ClientError, botocore.exceptions.PartialCredentialsError) as e:
        logging.error(f"Error getting object attributes for {s3_key}: {e}")
        sys.exit(1)

def localChecksumValidation(objectSummary, local_file_path):
    checksumAlgo = whichChecksum(objectSummary)
    if 'SHA' in checksumAlgo:
        return shaChecksums(objectSummary, local_file_path)
    if 'CRC' in checksumAlgo:
        return crcChecksums(objectSummary, local_file_path)

def crcChecksums(objectSummary, local_file_path):
    checksumAlgo = whichChecksum(objectSummary)
    try:
        if 'ObjectParts' in objectSummary:
            partOneSize = objectSummary['ObjectParts']['Parts'][0]['Size']
            CHUNK_SIZE = partOneSize
            partHashListBase64 = []

            with open(local_file_path, "rb") as f:
                chunk = f.read(CHUNK_SIZE)
                while chunk:
                    checksum = 0
                    if checksumAlgo == 'ChecksumCRC32':
                        m = zlib.crc32(chunk, checksum)
                    elif checksumAlgo == 'ChecksumCRC32C':
                        m = crc32c.crc32c(chunk)
                    m = m.to_bytes((m.bit_length() + 7) // 8, 'big') or b'\0'
                    partHashListBase64.append(m)
                    chunk = f.read(CHUNK_SIZE)

            concatStr = b''.join(partHashListBase64)
            if checksumAlgo == 'ChecksumCRC32':
                m = zlib.crc32(concatStr, checksum)
            elif checksumAlgo == 'ChecksumCRC32C':
                m = crc32c.crc32c(concatStr)
            m = m.to_bytes((m.bit_length() + 7) // 8, 'big') or b'\0'

            return base64.b64encode(m).decode('utf-8')
        else:
            with open(local_file_path, "rb") as f:
                fileData = f.read()
                if checksumAlgo == 'ChecksumCRC32':
                    checksum = 0
                    m = zlib.crc32(fileData, checksum)
                elif checksumAlgo == 'ChecksumCRC32C':
                    checksum = 0
                    m = crc32c.crc32c(fileData)
                m = m.to_bytes((m.bit_length() + 7) // 8, 'big') or b'\0'
                return base64.b64encode(m).decode('utf-8')
    except Exception as e:
        logging.error(f"Error calculating local checksum for {local_file_path}: {e}")
        sys.exit(1)

def shaChecksums(objectSummary, local_file_path):
    checksumAlgo = whichChecksum(objectSummary)
    try:
        if 'ObjectParts' in objectSummary:
            partOneSize = objectSummary['ObjectParts']['Parts'][0]['Size']
            CHUNK_SIZE = partOneSize
            partHashListBase64 = []

            with open(local_file_path, "rb") as f:
                chunk = f.read(CHUNK_SIZE)
                while chunk:
                    if checksumAlgo == 'ChecksumSHA256':
                        m = hashlib.sha256()
                    elif checksumAlgo == 'ChecksumSHA1':
                        m = hashlib.sha1()
                    m.update(chunk)
                    partHashListBase64.append(base64.b64encode(m.digest()))
                    chunk = f.read(CHUNK_SIZE)

            if checksumAlgo == 'ChecksumSHA256':
                m = hashlib.sha256()
            elif checksumAlgo == 'ChecksumSHA1':
                m = hashlib.sha1()
            for line in partHashListBase64:
                m.update(base64.b64decode(line))

            return base64.b64encode(m.digest()).decode('utf-8')
        else:
            with open(local_file_path, "rb") as f:
                fileData = f.read()
                if checksumAlgo == 'ChecksumSHA256':
                    m = hashlib.sha256()
                elif checksumAlgo == 'ChecksumSHA1':
                    m = hashlib.sha1()
                m.update(fileData)
            return base64.b64encode(m.digest()).decode('utf-8')
    except Exception as e:
        logging.error(f"Error calculating local checksum for {local_file_path}: {e}")
        sys.exit(1)

def s3checksumResult(objectSummary):
    checksumAlgo = whichChecksum(objectSummary)
    return objectSummary['Checksum'][checksumAlgo]

def traverse_and_validate():
    s3_client = boto3.client('s3')
    for root, _, files in os.walk(args.localDir):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_file_path, args.localDir).replace(os.path.sep, '/')
            try:
                objectSummary = getObjectAttributes(s3_client, args.bucketName, s3_key)
                s3Checksum = s3checksumResult(objectSummary)
                localChecksum = localChecksumValidation(objectSummary, local_file_path)
                if s3Checksum == localChecksum:
                    logging.info(f'PASS: {s3_key} - {whichChecksum(objectSummary)} match! s3Checksum: {s3Checksum} | localChecksum: {localChecksum}')
                else:
                    logging.warning(f'FAIL: {s3_key} - {whichChecksum(objectSummary)} DO NOT MATCH! s3Checksum: {s3Checksum} | localChecksum: {localChecksum}')
            except botocore.exceptions.ClientError as e:
                logging.error(f'ERROR: Could not process {s3_key}: {e}')

if __name__ == '__main__':
    traverse_and_validate()
