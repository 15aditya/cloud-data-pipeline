import boto3
from pyspark.sql.types import StringType, IntegerType, ShortType, TimestampType, StructField, StructType


def load_spark_dataframe_from_catalog(glue_context, logger, database, table_name, redshift_tmp_dir):
    """
    Creates spark dataframe from glue catalog even in case table is empty in Redshift
    :param glue_context: glue context
    :param logger: logger object
    :param database: database name
    :param table_name: table name
    :param redshift_tmp_dir: redshift temporary directory
    :return: spark dataframe
    """
    try:
        dynf = glue_context.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name,
            redshift_tmp_dir=redshift_tmp_dir)
        df = dynf.toDF()
        if df.columns:
            return df
    except Exception as ex:
        msg_line = str(ex).split('\n', 1)[0]
        if msg_line.find('getDynamicFrame') == -1:
            logger.error('Could not create dataframe from {}.{}: {}'.format(database, table_name, msg_line))
            raise ex
    type_map = {
        # TODO: add all types
        'string': StringType,
        'int': IntegerType,
        'smallint': ShortType,
        'timestamp': TimestampType
    }
    glue_client = boto3.client('glue', region_name='ca-central-1')
    resp = glue_client.get_table(DatabaseName=database, Name=table_name)
    col_defs = resp['Table']['StorageDescriptor']['Columns']
    schema = StructType([StructField(col['Name'], type_map[col['Type']]()) for col in col_defs])
    df = glue_context.sparkSession.createDataFrame([], schema)
    logger.warn('Empty dataset {}.{} created from catalog metadata.'.format(database, table_name))
    return df


def create_dataframe_from_options(gc, connection_type, connection, database, table, schema, temp_dir=None):
    """
    Creates spark dataframe from sqlserver or redshift table.
    :param gc: glue context
    :param connection_type: connection type could be sqlserver or redshift
    :param connection: glue connection name
    :param database: database name
    :param table: table name
    :param schema: schema name
    :param temp_dir: temporary directory location
    :return: spark dataframe
    """
    glue_jdbc_conf = gc.extract_jdbc_conf(connection)
    if connection_type == 'sqlserver':
        return gc.create_dynamic_frame.from_options(connection_type=connection_type,
                                                    connection_options={
                                                        "url": glue_jdbc_conf['url'] + ';' + 'databaseName=' + database,
                                                        "user": glue_jdbc_conf['user'],
                                                        "password": glue_jdbc_conf['password'],
                                                        "dbtable": schema+'.'+table},
                                                    format=None,
                                                    format_options={},
                                                    transformation_ctx="").toDF()
    elif connection_type == 'redshift':
        return gc.create_dynamic_frame.from_options(connection_type=connection_type,
                                                    connection_options={
                                                        "url": glue_jdbc_conf['url'] + '/' + database,
                                                        "user": glue_jdbc_conf['user'],
                                                        "password": glue_jdbc_conf['password'],
                                                        "dbtable": schema+'.'+table,
                                                        "redshiftTmpDir": temp_dir},
                                                    format=None,
                                                    format_options={},
                                                    transformation_ctx="").toDF()
    else:
        raise Exception('Not a valid connection type; Could be sqlserver or redshift.')
