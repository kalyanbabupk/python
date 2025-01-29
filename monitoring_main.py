import os
import json
# import asyncio
import logging
from datetime import datetime
import traceback
from google.cloud import monitoring_v3
from google.protobuf import duration_pb2
from google.oauth2 import service_account
import asyncpg

# Set up logging
logging.basicConfig(filename='error.log', level=logging.ERROR)

# Load GCP credentials
credentials = service_account.Credentials.from_service_account_file(
    'gcp-demo-platform.json',
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)

# Load database configuration from a JSON file
def load_db_config():
    config_path = os.path.join(os.getcwd(), "db_config.json")
    with open(config_path, "r") as f:
        # return json.load(f)["db"]
        return json.load(f)["db"]

# Load metric configuration from a JSON file
def get_metric_config():
    file_path = os.path.join(os.getcwd(), "metrics_config.json")
    with open(file_path, "r") as f:
        return json.load(f)

# Asynchronous function to insert data into PostgreSQL
async def insert_data(conn, data_list, schema_name, table_name):
    if not data_list:
        return  # Exit if there's no data to insert
    # Create batches of 1000 records
    batches = [data_list[i:i + 1000] for i in range(0, len(data_list), 1000)]
    print(f"Batches to insert: {len(batches)}")

    for batch in batches:
        try:
            await conn.execute(f"SET search_path TO {schema_name};")
            # Query the column names from the specified table

            query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}' ORDER BY ordinal_position
            """
            column_names = await conn.fetch(query)
            
            columns = [record['column_name'] for record in column_names]
            # Prepare records in tuple format
            records = [tuple(item[col] for col in columns) for item in batch]
            print(f"Inserting {len(records)} records for batch.")

            # Insert records using copy_records_to_table
            await conn.copy_records_to_table(table_name, records=records)

        except Exception as e:
            logging.error(f"Error inserting batch: {str(e)}")
            print(f"Error inserting batch: {str(e)}")



async def get_metrics(project_id, tenant_id, metric, start_time, end_time, db_config):
    client = monitoring_v3.MetricServiceClient(credentials=credentials)
    project_name = f"projects/{project_id}"
    # metric_unit=get_metric_descriptor_unit(client, project_id, metric)
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
    interval = monitoring_v3.TimeInterval({
        "end_time": {"seconds": int(end_time.timestamp())},
        "start_time": {"seconds": int(start_time.timestamp())},
    })

    conn = await asyncpg.connect(
        user=db_config['user'],
        password=db_config['password'],
        database=db_config['database'],
        host=db_config['host']
    )
    metric_unit = metric['Unit']
    convertion = metric['Convertion']
    async with conn.transaction():
        for aggregation in metric["Aggregation"]:
            aligner = aggregation
            data_list = []

            # Define alignment period based on the Interval
            try:
                if metric["Interval"] == "Hourly":
                    alignment_sec = 3600
                elif metric["Interval"] == "Daily":
                    alignment_sec = 86400
                elif metric["Interval"] == "Quarterly":
                    alignment_sec = 7776000
                else:
                    alignment_sec = 60

                alignment_period = duration_pb2.Duration(seconds=alignment_sec)
            except Exception as e:
                logging.error(f"Error defining alignment period for {metric['MetricType']}: {str(e)} at line {traceback.extract_tb(e.__traceback__)[-1].lineno}")
                continue

            try:
                response = client.list_time_series(
                    request={
                        "name": project_name,
                        "filter": f'metric.type = "{metric["MetricType"]}"',
                        "interval": interval,
                        "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                        "aggregation": {
                            "alignment_period": alignment_period,
                            "per_series_aligner": monitoring_v3.Aggregation.Aligner[aligner],
                        },
                    }
                )
                
                for series in response:
                    for point in series.points:
                        resource_labels = series.resource.labels
                        resource_labels['resource_type'] = series.resource.type
                        metric_labels = series.metric.labels
                        metric_labels['metric_type'] = series.metric.type
                        usage_datetime=datetime.strptime(str(point.interval.end_time), "%Y-%m-%d %H:%M:%S%z").replace(tzinfo=None)
                        data = {
                            "tenant_id": tenant_id,
                            "project_id": resource_labels.get("project_id", "N/A"),
                            "zone": resource_labels.get("zone", "N/A"),
                            "entry_timestamp": datetime.now(),
                            "metric_namespace": series.resource.type,
                            "metric_type": series.metric.type,
                            "aggregation": aligner,
                            "usage_datetime": usage_datetime,
                            "resource_labels": json.dumps({k: v for k, v in resource_labels.items()}),  
                            "metric_labels": json.dumps({k: v for k, v in metric_labels.items()}), 
                            "datapoints": eval(str(point.value.double_value)+convertion),
                            "unit": metric_unit,
                        }
                        data_list.append(data)

            except Exception as e:
                logging.error(f"Error fetching time series data for {metric['MetricType']}: {str(e)} at line {traceback.extract_tb(e.__traceback__)[-1].lineno}")
                continue

            # Insert all collected data for this metric at once
            try:
                await insert_data(conn, data_list, "metrics", "cw_metrics_etl_txn")
            except Exception as e:
                logging.error(f"Error inserting data for metric {metric['MetricType']}: {str(e)} at line {traceback.extract_tb(e.__traceback__)[-1].lineno}")

    await conn.close()  # Close the connection when done


# Main function to orchestrate metric fetching
async def main():
    db_config = load_db_config()
    project_ids = ["prj-ce-p-monitoring-392109"]  # Replace with your project ID
    metric_list = get_metric_config()
    start_time = "2024-09-01 08:00:00"
    end_time = "2024-10-01 00:00:00"
    tenant_id = 24
    for project_id in project_ids:
        for metric in metric_list:
            await get_metrics(project_id, tenant_id, metric, start_time, end_time, db_config)

# Entry point
if __name__ == "__main__":
     main()
