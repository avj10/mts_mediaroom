from configparser import ConfigParser
from os.path import abspath
import json
from pyspark.sql import HiveContext, SQLContext, SparkSession
from pyspark import SparkConf, SparkContext
import logging
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import udf, date_format, lit, col, substring, unix_timestamp
import subprocess
import time
import datetime

warehouse_location = abspath('spark-warehouse')
conf = SparkConf().setAppName('Mediaroom Staging')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName('Mediaroom Staging') \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "cluster") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set("spark.dynamicAllocation.enabled", "True")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

logging.basicConfig(level=logging.INF0, format='%(asctime)s | %(name)s | %(levelname)s | %(message)s)')
logging.basicConfig(level=logging.INFO)


def run_cmd(args_list):
    logging.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def read_config_file(config_name):
    parser = ConfigParser()
    parser.read('config_data.ini')
    # Read user input from 'config_data.ini' file.
    if config_name == "hive_tables_config":
        config = parser['hive_tables_config']
    else:
        config = None
    return config


def read_param_file():
    with open('params.json') as j:
        table_dict = json.load(j)
    return table_dict


def lower_case_columns(df):
    for temp_col in df.columns:
        df = df.withColumnRenamed(temp_col, temp_col.lower())
    return df


def rename_cols(df):
    try:
        df = df.withColumnRenamed('run_id', 'loaddate')
    except Exception as e:
        pass
    return df


def get_current_timestamp(df):
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    df_lit = df.withColumn("hadoop_load_dtm", unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    df_dated = df_lit.withColumn('hadoop_load_dtm', col('hadoop_load_dtm').cast(StringType()))
    return df_dated


def get_year_month_day(df):
    df_Ymd = df.withColumn('year', substring('dayid', 1, 4)) \
        .withColumn('month', substring('dayid', 5, 2)) \
        .withColumn('day', substring('dayid', 7, 2))

    df_Ymd = df_Ymd.withColumn('year', col('year').cast(IntegerType())) \
        .withColumn('month', col('month').cast(IntegerType())) \
        .withColumn('day', col('day').cast(IntegerType()))

    return df_Ymd


def landing_to_staging():
    try:
        # Reading config file to get hive table details
        table_dict = read_param_file()
        hive_config = read_config_file("hive_tables_config")
        base_landing_path = hive_config['base_landing_path']
        base_staging_path = hive_config['base_staging_path']
        region_name = hive_config['region_name']
        for i in range(0, len(table_dict)):
            sql_table_name = table_dict[i]["table_name"]
            partition_column_name = table_dict[i]["partition"]
            target_hdfs_path = base_landing_path + "mrdw_" + region_name + "_" + table_dict[i]["table_name"] + "_lnd/"
            log_id = region_name + '_' + sql_table_name + ':'
            logging.info(log_id + 'starting landing to stg dump')
            try:
                df = spark.read.parquet(target_hdfs_path)
            except:
                logging.warning(
                    log_id + "No data ingested for %s, %s, %s" % (region_name, sql_table_name, target_hdfs_path))
                return 1

            # Lower case col names
            df_lower = lower_case_columns(df)
            # Add current timestamp for audit purposes
            df_dated = get_current_timestamp(df_lower)
            # If data is partitioned by dayid, extract year, month,day as these will be partitions in final database
            if partition_column_name == 'dayid':
                df_ymd = get_year_month_day(df_dated)
            else:
                df_ymd = df_dated

            # Rename col where run_id
            if 'run_id' in df_ymd.columns:
                df_renamed = rename_cols(df_ymd)
            else:
                df_renamed = df_ymd

            logging.info(log_id + ' cleaning up stg db')
            stg_hdfs_path = base_staging_path + "mrdw_" + region_name + "_" + table_dict[i]["table_name"] + "_stg/*"
            (ret, out, err) = run_cmd(['hadoop', 'fs', '-rm', '-r', stg_hdfs_path])
            if ret == 0:
                # creating temporary table to ingesting in staging layer
                df_renamed.createOrReplaceTempView('intermediate')

                # Insert overwrite into staging db partitioned by year month day as in final database
                if partition_column_name == 'dayid':
                    insert_cmd = 'INSERT OVERWRITE TABLE brsbi_staging.' + region_name + '_' + sql_table_name \
                                 + '_stg partition(year, month, day) select * from intermediate'
                else:
                    insert_cmd = 'INSERT OVERWRITE TABLE brsbi_staging.' + region_name + '_' + sql_table_name \
                                 + '_stg select * from intermediate'
                spark.sql(insert_cmd)
                spark.sql('MSCK REPAIR TABLE brsbi_staging.' + region_name + '_' + sql_table_name + '_stg')
                logging.info(log_id + ' successfully inserted data into stg db')
            else:
                logging.info(log_id + ' Getting problem while deleting stg table and error is :' + err)
                raise ValueError("Getting problem while deleting stg table and error is :" + err)
        return 0
    except Exception as e:
        logging.info("The error in delta_to_landing function: " + e)
        return 1
