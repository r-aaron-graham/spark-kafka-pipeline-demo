from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, IntegerType, DoubleType, TimestampType
import yaml

# Load config
config = yaml.safe_load(open('config.yaml'))

def main():
    spark = SparkSession.builder.appName('SparkKafkaDemo').getOrCreate()
    schema = StructType() \
        .add('id', IntegerType()) \
        .add('value', DoubleType()) \
        .add('ts', DoubleType())

    df = (
        spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers', config['kafka']['bootstrap_servers'])
            .option('subscribe', config['kafka']['topic'])
            .load()
    )
    json_df = df.selectExpr("CAST(value AS STRING) as json")
    parsed = json_df.select(from_json(col('json'), schema).alias('data')).select('data.*')
    result = parsed.withColumn('timestamp', to_timestamp(col('ts')))
    
    query = (result.writeStream
             .format('parquet')
             .option('path', config['sink']['path'])
             .option('checkpointLocation', config['sink']['checkpoint'])
             .outputMode('append')
             .start())
    query.awaitTermination()

if __name__ == '__main__':
    main()
