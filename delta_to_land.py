import logging
import subprocess
from datetime import datetime, timedelta, date
from configparser import ConfigParser
import json
from os.path import abspath
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, SQLContext, SparkSession

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


def read_config_file(config_name):
    parser = ConfigParser()
    parser.read('config_data.ini')
    # Read user input from 'config_data.ini' file.
    if config_name == "sql_config":
        config = parser['sql_config']
    elif config_name == "hadoop_config":
        config = parser['hadoop_config']
    elif config_name == "hive_tables_config":
        config = parser['hive_tables_config']
    else:
        config = None
    return config


def read_param_file():
    with open('params.json') as j:
        table_dict = json.load(j)
    return table_dict


def run_cmd(args_list):
    logging.info('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def setup_spark_connection_properties():
    # calling read_config_file()
    SQL_CONFIG = read_config_file()
    # SQL SERVER CONSTANTS
    driver_name = SQL_CONFIG['SQL_driver']
    host_name = SQL_CONFIG['SQL_hostname']
    username = SQL_CONFIG['SQL_username']
    password = SQL_CONFIG['SQL_password']
    db_name = SQL_CONFIG['SQL_dbname']
    port = SQL_CONFIG['SQL_port']

    # SQL Server Connection link
    jdbc_sql_server = "jdbc:sqlserver://{0}:{1};database={2}".format(host_name, port, db_name)
    spark_connection_properties = {"user": username, "password": password, "driver": driver_name}
    return db_name, jdbc_sql_server, spark_connection_properties


def get_list_of_partitions(node_path):
    hadoop_config = read_config_file("hadoop_config")
    NODE_01 = hadoop_config['NODE_01']
    NODE_02 = hadoop_config['NODE_02']
    ret = 1
    while ret != 0:
        node_1 = NODE_01 + node_path
        (ret, out, err) = run_cmd(['hdfs', 'dfs', '-ls', node_1])
        working_node_path = node_1
        if ret == 1:
            # First node is busy, using another node
            node_2 = NODE_02 + node_path
            (ret, out, err) = run_cmd(['hdfs', 'dfs', '-ls', node_2])
            working_node_path = node_2

    return ret, out, err, working_node_path


def get_last_partition_value_from_hdfs(source_path):
    source_ret, source_out, source_err, source_path = get_list_of_partitions(source_path)
    if source_ret == 0:
        source_out = source_out.decode("utf-8")
        if source_out == '':
            logging.info("Landing is empty so considering last value as 7 days ago date.")
            today = date.today()
            last_date = today - timedelta(days=7)
            last_value = last_date.strftime("%Y%m%d")
        else:
            list_of_partitions = source_out.split('=')
            last_date_of_partition = list_of_partitions[-1]
            last_value = last_date_of_partition[0:8]
        return last_value, source_path
    else:
        raise ValueError("Last Value Not Found from HDFS path")


def get_maximum_partition_value_from_sql_server(table_name, column_name):
    try:
        # calling setup_spark_connection_properties()
        db_name, jdbc_sql_server, spark_connection_properties = setup_spark_connection_properties()
        query = "(SELECT DISTINCT " + column_name + " FROM " + db_name + ".dbo." + table_name + ") mytable"
        # Reading SQL Server table into spark dataframe using PySpark
        df = spark.read.jdbc(url=jdbc_sql_server, table=query, properties=spark_connection_properties)
        result = df.select([max(column_name)])
        # getting max value in variable
        max_value = result.head()[0]
        if max_value > 0:
            return max_value
        raise ValueError("Max value not found from table:" + table_name + " in SQL Server.")
    except Exception as e:
        logging.info(e)


def run_sqoop_import_command(table_name, column_name, last_value, max_value, target_dir):
    # calling read_config_file()
    SQL_CONFIG = read_config_file("sql_config")
    # SQL SERVER CONSTANTS
    host_name = SQL_CONFIG['SQL_hostname']
    username = SQL_CONFIG['SQL_username']
    password = SQL_CONFIG['SQL_password']
    db_name = SQL_CONFIG['SQL_dbname']
    port = SQL_CONFIG['SQL_port']
    jdbc_sql_server = "jdbc:sqlserver://{0}:{1};database={2}".format(host_name, port, db_name)

    if column_name == 'dayid':
        where_value = 'dayid=' + max_value
        target_dir = target_dir + where_value
        (ret, out, out2) = run_cmd(['sqoop', 'import',
                                    '--connect', jdbc_sql_server,
                                    '--username', username,
                                    '--password', password,
                                    '--table', table_name,
                                    '--where', where_value,
                                    '--split-by', column_name,
                                    '--target-dir', target_dir,
                                    '-m', '1', '--as-parquetfile', '--incremental', 'append',
                                    '--check-column', column_name,
                                    '--last-value', str(last_value)])
    else:
        (ret, out, out2) = run_cmd(['sqoop', 'import',
                                    '--connect', jdbc_sql_server,
                                    '--username', username,
                                    '--password', password,
                                    '--table', table_name,
                                    '--target-dir', target_dir,
                                    '-m', '1', '--as-parquetfile', '--delete-target-dir'])

    out2 = out2.decode("utf-8")
    out = out.decode("utf-8")
    if out.find('successfully') != -1 or out2.find('successfully') != -1:
        logging.info("Data loaded successfully for table name:" + table_name)
        return 0
    else:
        logging.info("Sqoop Fail to load data for table name:" + table_name)
        return 1


def delta_to_landing():
    try:
        # Reading config file to get hive table details
        table_dict = read_param_file()
        hive_config = read_config_file("hive_tables_config")
        base_landing_path = hive_config['base_landing_path']
        region_name = hive_config['region_name']
        for i in range(0, len(table_dict)):
            sql_table_name = table_dict[i]["table_name"]
            partition_column_name = table_dict[i]["partition"]
            target_hdfs_path = base_landing_path + "mrdw_" + region_name + "_" + table_dict[i]["table_name"] + "_lnd/"
            if partition_column_name == 'dayid':
                # getting last value from HDFS for given target path
                last_value, target_hdfs_path = get_last_partition_value_from_hdfs(target_hdfs_path)
                # getting maximum value from SQL Server for given table name and column value
                max_value = get_maximum_partition_value_from_sql_server(sql_table_name, partition_column_name)

                last_value = str(last_value)
                max_value = str(max_value)
                last_date = datetime(year=int(last_value[0:4]), month=int(last_value[4:6]),
                                     day=int(last_value[6:8]))
                max_date = datetime(year=int(max_value[0:4]), month=int(max_value[4:6]),
                                    day=int(max_value[6:8]))

                no_of_days = max_date - last_date
                no_of_days = str(no_of_days)
                no_of_days = no_of_days.split(' ')
                no_of_days = no_of_days[0]
                no_of_days = int(no_of_days)
                temp_max_date = last_date
                for x in range(no_of_days):
                    temp_max_date += timedelta(days=1)
                    temp_max_value = temp_max_date.strftime("%Y%m%d")

                    # passing all values to sqoop import command to fetch the data
                    data_load_check = run_sqoop_import_command(sql_table_name, partition_column_name, last_value,
                                                               temp_max_value, target_hdfs_path)
                    if data_load_check == 0:
                        spark.sql('MSCK REPAIR TABLE brsbi_landing.' + region_name + '_' + sql_table_name)
                    else:
                        raise ValueError("There is some problem to load data from table:" + str(sql_table_name))
            else:
                data_load_check = run_sqoop_import_command(sql_table_name, partition_column_name, 0, 0,
                                                           target_hdfs_path)
                if data_load_check == 0:
                    spark.sql('MSCK REPAIR TABLE brsbi_landing.' + region_name + '_' + sql_table_name)
                else:
                    raise ValueError("There is some problem to load data from table:" + str(sql_table_name))
        return 0
    except Exception as e:
        logging.info("The error in delta_to_landing function: " + e)
        return 1
