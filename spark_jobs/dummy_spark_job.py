import logging
import os
import sys

from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_date, year, month, current_timestamp, lit

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(filename)s: %(message)s')

def main():
    if len(sys.argv) != 5:
        logger.info('Incorrect arguments provided. Correct usage: gcs_to_bigquery.py <input_path> <output_dataset> <output_table> <temp_bucket>')
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_dataset = sys.argv[2]
    output_table = sys.argv[3]
    temp_bucket = sys.argv[4]
    
    logger.info(f'Input path: {input_path}')
    logger.info(f'Output dataset: {output_dataset}')
    logger.info(f'Output table: {output_table}')
    logger.info(f'Temp bucket: {temp_bucket}')
    
    logger.info('Initializing Spark session')
    spark = SparkSession.builder \
        .appName('GCS to BigQuery ETL') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.31.1.jar') \
        .getOrCreate()
    
    # spark.sparkContext.setLogLevel('WARN')
    
    try:
        logger.info(f'Spark version: {spark.version}')

        logger.info(f'Attempting to read: {input_path}')
        # df = spark.read.option('header', 'true') \
        #                 .option('inferSchema', 'true') \
        #                 .csv(input_path)
        df = spark.read \
        .options(
            delimiter=';',
            header=True,
            inferSchema=True,
            encoding='UTF-8',
            dateFormat='dd/MM/yyyy'
        ) \
        .csv(input_path)
        
        print(f'Successfully read data from {input_path}')
        print(f'Schema: {df.schema}')
        print(f'Row count: {df.count()}')

        logger.info('Sample data:')
        df.show(5, truncate=False)
        
        df.write \
            .format('bigquery') \
            .option('table', f'{output_dataset}.{output_table}') \
            .option('temporaryGcsBucket', temp_bucket.replace('gs://', '')) \
            .option('writeMethod', 'direct') \
            .mode('append') \
            .save()
        
        print(f'Successfully wrote data to BigQuery table {output_dataset}.{output_table}')
        
    except Exception as e:
        print(f'Error processing data: {str(e)}')
        raise
    
    # Clean up
    spark.stop()

if __name__ == '__main__':
    main()