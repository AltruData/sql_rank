import argparse
import boto3
import pandas as pd
from io import BytesIO

def load_dataframe_from_s3(access_key, secret_key, bucket_name, object_name):
    """
    Load a DataFrame from an object in S3.

    :param access_key: AWS access key ID
    :param secret_key: AWS secret access key
    :param bucket_name: Name of the S3 bucket
    :param object_name: Name of the object in the S3 bucket
    :return: DataFrame loaded from the S3 object
    """
    # Initialize a session using your credentials
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Create an S3 client using the session
    s3 = session.client('s3')

    # Get the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    
    # Load the object's body into a pandas DataFrame
    dataframe = pd.read_csv(BytesIO(response['Body'].read()))

    return dataframe

def calculate_mean(dataframe, column_name):
    """
    Calculate the mean of a specified column in the DataFrame.

    :param dataframe: DataFrame to analyze
    :param column_name: Column to calculate the mean
    :return: Mean of the column
    """
    return dataframe[column_name].mean()

def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description='Load a DataFrame from S3 and calculate the mean of a column.')
    parser.add_argument('--awskey', type=str, required=True, help='AWS access key ID')
    parser.add_argument('--secret', type=str, required=True, help='AWS secret access key')
    parser.add_argument('--bucket', type=str, required=True, help='S3 bucket name')
    parser.add_argument('--object', type=str, required=True, help='Object name in the S3 bucket')
    parser.add_argument('--column', type=str, required=True, help='Column name to calculate mean')
    
    # Parse arguments
    args = parser.parse_args()

    # Load the DataFrame from S3
    df = load_dataframe_from_s3(args.awskey, args.secret, args.bucket, args.object)

    # Calculate the mean of the specified column
    mean_value = calculate_mean(df, args.column)

    # Print the result
    print(f"The mean of column '{args.column}' is: {mean_value}")

if __name__ == "__main__":
    main()
