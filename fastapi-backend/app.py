from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session
from datetime import datetime, timedelta
import pydoop.hdfs as hdfs
from pyhive import hive
import json
import os
import re
import subprocess
import threading

# Database configuration
DATABASE_URL = "postgresql://hudi_user:password@localhost/hudi_bootstrap_db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class HudiTransaction(Base):
    __tablename__ = "hudi_transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    status = Column(String, default="PENDING")  # PENDING, FAILED, SUCCESS
    transaction_data = Column(Text, nullable=False)
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    app_id = Column(String, nullable=True)
    error_log = Column(Text, nullable=True)  # Store error log if any

Base.metadata.create_all(bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Consider specifying allowed origins for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class HudiBootstrapRequest(BaseModel):
    data_file_path: str
    hudi_table_name: str
    key_field: str
    precombine_field: str
    partition_field: Optional[str] = None
    hudi_table_type: str  # COPY_ON_WRITE or MERGE_ON_READ
    output_path: str
    spark_config: Optional[dict] = None  # Optional spark configurations
    bootstrap_type: str  # FULL_RECORD or METADATA_ONLY
    partition_regex: Optional[str] = None  # Optional regex for partitions

def run_spark_submit(request: HudiBootstrapRequest, transaction: HudiTransaction):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    pyspark_script_path = os.path.join(current_directory, "pyspark_script.py")
    log_file_path = os.path.join(current_directory, f"pyspark_script_{transaction.transaction_id}.log")

    # Build the spark-submit command
    spark_submit_command = [
        "spark-submit",
        "--master", "local"
    ]
    
    # Add Spark configurations
    if request.spark_config:
        for key, value in request.spark_config.items():
            spark_submit_command.append(f"--conf")
            spark_submit_command.append(f"{key}={value}")
    
    spark_submit_command.append(pyspark_script_path)
    spark_submit_command.append(f"--data-file-path={request.data_file_path}")
    spark_submit_command.append(f"--hudi-table-name={request.hudi_table_name}")
    spark_submit_command.append(f"--key-field={request.key_field}")
    spark_submit_command.append(f"--precombine-field={request.precombine_field}")
    if request.partition_field:
        spark_submit_command.append(f"--partition-field={request.partition_field}")

    spark_submit_command.append(f"--hudi-table-type={request.hudi_table_type}")
    spark_submit_command.append(f"--output-path={request.output_path}")
    spark_submit_command.append(f"--bootstrap-type={request.bootstrap_type}")

    if request.partition_regex:
        spark_submit_command.append(f"--partition-regex={request.partition_regex}")

    spark_submit_command.append(f"--log-file={log_file_path}")

    # Call Spark-submit
    process = subprocess.Popen(spark_submit_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    app_id_match = re.search(r'local-\d{13}', stderr.decode())
    if app_id_match:
        app_id = app_id_match.group(0)
        transaction.app_id = app_id
        transaction.end_time = None
        save_transaction(transaction)

    # Log output and errors
    with open(log_file_path, "r") as log_file:
        error_log = log_file.read()
        if process.returncode != 0:
            transaction.status = "FAILED"
            transaction.error_log = error_log
            os.remove(log_file_path)
        else:
            transaction.status = "SUCCESS"
            transaction.error_log = error_log
            os.remove(log_file_path)

    transaction.end_time = datetime.utcnow()
    save_transaction(transaction)

def save_transaction(transaction: HudiTransaction):
    try:
        with SessionLocal() as db:
            db.add(transaction)
            db.commit()
            db.refresh(transaction)
    except Exception as e:
        print(f"Error saving transaction: {e}")

@app.post("/bootstrap_hudi/")
def bootstrap_hudi(request: HudiBootstrapRequest, db: Session = Depends(get_db)):
    transaction_id = f"{request.hudi_table_name}-{int(datetime.utcnow().timestamp())}"
    transaction = HudiTransaction(
        transaction_id=transaction_id,
        status="PENDING",
        transaction_data=json.dumps(request.dict()),
        start_time=datetime.utcnow(),
    )
    save_transaction(transaction)

    # Run the Spark submit in a separate thread
    thread = threading.Thread(target=run_spark_submit, args=(request, transaction))
    thread.start()

    return {"transaction_id": transaction.transaction_id, "message": "Bootstrapping started."}

@app.get("/bootstrap_history/")
def get_bootstrap_history(start_date: Optional[str] = None, end_date: Optional[str] = None, transaction_id: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(HudiTransaction)
    
    if transaction_id:
        query = query.filter(HudiTransaction.transaction_id.like(f"%{transaction_id}%"))
    
    if start_date:
        query = query.filter(HudiTransaction.start_time >= datetime.fromisoformat(start_date))
    
    if end_date:
        end_date_obj = datetime.fromisoformat(end_date)
        end_time = end_date_obj + timedelta(days=1)
        query = query.filter(HudiTransaction.start_time < end_time)
    
    transactions = query.order_by(HudiTransaction.start_time.desc()).all()
    return transactions

@app.get("/bootstrap_status/{transaction_id}/")
async def bootstrap_status(transaction_id: str, db: Session = Depends(get_db)):
    transaction = db.query(HudiTransaction).filter(HudiTransaction.transaction_id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found.")
    error_message=None
    if transaction.error_log:
    	error_message = parse_error_log(transaction.error_log)
    return {
        "status": transaction.status,
        "error_log": transaction.error_log,
        "error_message": error_message
    }

def parse_error_log(error_log: str) -> str:
    """Parse error log for meaningful messages."""
    if "Configuration Error:" in error_log:
        return "Configuration Error: " + error_log.split("Configuration Error:")[1].strip().split("\n")[0]
    elif "Permission Denied:" in error_log:
        return "Access Permission Error: " + error_log.split("Permission Denied:")[1].strip().split("\n")[0]
    elif "Unsupported file format:" in error_log:
        return "Unsupported File Format: Only .parquet and .orc files are supported."
    else:
        return "An Unexpected error occurred during Hudi table Bootstrap"

class PathOrTableCheck(BaseModel):
    hudi_table_name: str

@app.post("/check_path_or_table/")
async def check_path_or_table(data: PathOrTableCheck, db: Session = Depends(get_db)):
    """Check if the provided path is a HDFS path or a Hive table, and determine partitioning."""
    path_or_table = data.hudi_table_name  # Assuming this is the table name

    if path_or_table.startswith("hdfs://"):
        # Validate HDFS path
        is_partitioned = check_hdfs_partition(path_or_table)
        return {
            "isPartitioned": is_partitioned,
            "tableName": None,
            "hdfsLocation": path_or_table
        }
    else:
        table_name = check_hive_table(path_or_table)
        if table_name:
            hdfs_location = get_hdfs_location_from_hive_table(table_name)
            if hdfs_location:
                is_partitioned = check_hdfs_partition(hdfs_location)
                return {
                    "isPartitioned": is_partitioned,
                    "tableName": table_name,
                    "hdfsLocation": hdfs_location
                }
            else:
                raise HTTPException(status_code=404, detail="HDFS location for the Hive table not found.")
        else:
            raise HTTPException(status_code=404, detail="Hive table not found.")


hive_host = 'localhost'  # Update with your Hive server host
hive_port = 10000  # Default Hive port
hive_conn = hive.Connection(host=hive_host, port=hive_port)

def check_hive_table(table_name: str) -> Optional[str]:
    """Check if the specified Hive table exists and return its name."""
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            tables = cursor.fetchall()

        if tables and table_name in [t[0] for t in tables]:
            return table_name
        else:
            return None
    except Exception as e:
        print("Error checking Hive table:", e)
        return None

def get_hdfs_location_from_hive_table(table_name: str) -> Optional[str]:
    """Retrieve the HDFS location of the specified Hive table."""
    try:
        with hive_conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE FORMATTED {table_name}")
            rows = cursor.fetchall()
            print("Describe formatted output:", rows)  # Print the output for debugging
            
            for row in rows:
                if isinstance(row, tuple) and row:
                    # Check for 'Location:' first
                    if "Location:" in row[0]:
                        return row[1].strip()  # Return the HDFS location
                    # Also check for 'path' under Storage Desc Params
                    if "path" in row[0].lower():
                        return row[1].strip()  # Return the HDFS path

            print(f"No location found in DESCRIBE FORMATTED output for table {table_name}.")
        return None  # Location not found
    except Exception as e:
        print("Error retrieving HDFS location from Hive table:", e)
        return None



def check_hdfs_partition(hdfs_path: str) -> bool:
    """Check if the specified HDFS path is partitioned by looking for subdirectories and validate file formats."""
    try:
        # Check if the path exists
        if not hdfs.path.isfile(hdfs_path) and not hdfs.path.isdir(hdfs_path):
            raise HTTPException(status_code=404, detail=f"HDFS path does not exist: {hdfs_path}")

        has_partitions = False
        valid_formats = {'.parquet', '.orc'}
        found_valid_files = False

        def check_path(path: str):
            nonlocal has_partitions, found_valid_files
            # List contents of the HDFS directory
            files = hdfs.ls(path)

            for item in files:
                item_path = hdfs.path.join(path, item)
                if hdfs.path.isdir(item_path):
                    has_partitions = True
                    # Recursively check this partition for valid files
                    check_path(item_path)
                elif hdfs.path.isfile(item_path):
                    # Check for valid file formats
                    if any(item.endswith(fmt) for fmt in valid_formats):
                        found_valid_files = True

        # Start checking the initial path
        check_path(hdfs_path)

        # Determine final conditions
        if not found_valid_files and not has_partitions:
            raise HTTPException(status_code=404, detail=f"No files or directories found in the HDFS path: {hdfs_path}")
        elif not found_valid_files:
            raise HTTPException(status_code=404, detail=f"No valid files found in the HDFS path: {hdfs_path}")

        return has_partitions

    except HTTPException as e:
        raise e  # Re-raise HTTPException to maintain status code
    except Exception as e:
        print("Error checking HDFS partition:", e)
        raise HTTPException(status_code=500, detail="Internal server error")










# Uncomment the next line to run the server locally.
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)
