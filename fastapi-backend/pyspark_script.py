from pyspark.sql import SparkSession
import argparse
from pathlib import Path
import pydoop.hdfs as hdfs
import os
import re

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Bootstrap Hudi Table using DataSource Writer")
parser.add_argument("--data-file-path", required=True, help="Directory path containing the data files")
parser.add_argument("--hudi-table-name", required=True, help="Name of the Hudi table")
parser.add_argument("--key-field", required=True, help="Comma-separated list of key fields")
parser.add_argument("--precombine-field", required=True, help="Field used for pre-combining")
parser.add_argument("--partition-field", required=True, help="Partition field")
parser.add_argument("--hudi-table-type", required=True, help="Hudi table type (e.g., COW, MOR)")
parser.add_argument("--write-operation", required=True, help="Write operation (e.g., upsert, insert)")
parser.add_argument("--output-path", required=True, help="Output path for the Hudi table")
parser.add_argument("--bootstrap-type", required=True, help="Bootstrap type (e.g., full_record)")
parser.add_argument("--partition-regex", default="", help="Optional regex for partitioning")
args = parser.parse_args()

# Split key fields into list
key_fields = args.key_field.split(",")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Hudi Bootstrap") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

def get_first_file_extension(hdfs_path):
    """Retrieve the extension of the first file found in HDFS."""
    files = hdfs.ls(hdfs_path)
    for item in files:
        if hdfs.path.isfile(item):
            return Path(item).suffix.lower()
        elif hdfs.path.isdir(item):
            file_extension = get_first_file_extension(item)
            if file_extension:
                return file_extension
    return None

# Get the file extension of the first file
file_extension = get_first_file_extension(args.data_file_path)
# Read input data based on detected file format
if file_extension == ".parquet":
    input_df = spark.read.format("parquet").load(args.data_file_path)
elif file_extension == ".orc":
    input_df = spark.read.format("orc").load(args.data_file_path)
else:
    raise ValueError("Unsupported file format. Please provide a .parquet or .orc file.")

# Write to Hudi using the DataSource API for bootstrapping
try:
    input_df.write.format("hudi") \
        .option("hoodie.datasource.write.operation", args.write_operation) \
        .option("hoodie.datasource.write.table.type", args.hudi_table_type) \
        .option("hoodie.datasource.write.recordkey.field", ",".join(key_fields)) \
        .option("hoodie.datasource.write.precombine.field", args.precombine_field) \
        .option("hoodie.datasource.write.partitionpath.field", args.partition_field) \
        .option("hoodie.bootstrap.mode", args.bootstrap_type) \
        .option("hoodie.table.name", args.hudi_table_name) \
        .option("hoodie.upsert.shuffle.parallelism", 2) \
        .mode("Overwrite") \
        .save(args.output_path)

    print("Hudi table bootstrapped successfully.")

except Exception as e:
    print(f"Failed to bootstrap Hudi table: {e}")

finally:
    # Stop the Spark session
    spark.stop()
