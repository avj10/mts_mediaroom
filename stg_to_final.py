from configparser import ConfigParser
from os.path import abspath
import json
from pyspark.sql import HiveContext, SQLContext, SparkSession
from pyspark import SparkConf, SparkContext
import logging
import subprocess

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


def staging_to_final():
    try:
        # Reading config file to get hive table details
        table_dict = read_param_file()
        hive_config = read_config_file("hive_tables_config")
        base_staging_path = hive_config['base_staging_path']
        base_final_path = hive_config['base_final_path']
        region_name = hive_config['region_name']
        for i in range(0, len(table_dict)):
            sql_table_name = table_dict[i]["table_name"]
            partition_column_name = table_dict[i]["partition"]
            target_hdfs_path = base_staging_path + "mrdw_" + region_name + "_" + table_dict[i]["table_name"] + "_stg/"
            log_id = region_name + '_' + sql_table_name + ':'
            logging.info(log_id + 'starting landing to stg dump')
            try:
                df = spark.read.parquet(target_hdfs_path)
            except:
                logging.warning(
                    log_id + "No data ingested for %s, %s, %s" % (region_name, sql_table_name, target_hdfs_path))
                return 0

            # creating temporary table to ingesting in staging layer
            df.createOrReplaceTempView('intermediate_final')

            # Insert overwrite into final iptv db partitioned by year month day as in final database
            if partition_column_name == 'dayid':
                insert_cmd = 'INSERT TABLE iptv.' + region_name + '_' + sql_table_name \
                             + ' partition(year, month, day) select * from intermediate_final'
            else:
                insert_cmd = 'INSERT TABLE iptv.' + region_name + '_' + sql_table_name \
                             + ' select * from intermediate_final'
            spark.sql(insert_cmd)
            spark.sql('MSCK REPAIR TABLE iptv.' + region_name + '_' + sql_table_name)
            logging.info(log_id + ' successfully inserted data into final IPTV db')
        return 0
    except Exception as e:
        logging.info("The error in delta_to_landing function: " + e)
        return 1
