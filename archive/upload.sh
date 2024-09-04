#!/bin/bash

BUCKET_NAME=""
LOCAL_DIR=""
ALGORITHM="CRC32C"

for file in $(find "$LOCAL_DIR" -type f); do
  RELATIVE_PATH="${file#$LOCAL_DIR/}"
  aws s3api put-object --bucket $BUCKET_NAME --key $RELATIVE_PATH --body $file --checksum-algorithm $ALGORITHM > /dev/null 2>&1
done
