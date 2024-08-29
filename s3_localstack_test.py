import boto3

endpoint_url = "http://localhost.localstack.cloud:4566"
# alternatively, to use HTTPS endpoint on port 443:
# endpoint_url = "https://localhost.localstack.cloud"

def main():
    client = boto3.client("s3", endpoint_url=endpoint_url)
    result = client.list_buckets()

# Extract bucket names
    buckets = result.get('Buckets', [])

    # Print bucket names cleanly
    if buckets:
        print("S3 Buckets:")
        for bucket in buckets:
            print(f"- {bucket['Name']}")
    else:
        print("No S3 buckets found.")

if __name__ == "__main__":
    main()