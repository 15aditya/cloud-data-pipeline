"""
Description:
    This program reads data from source area (S3) and loads it into staging area (S3)
"""
import os
from awsglue.dynamicframe import DynamicFrame
from lib import file_operation, data_transform
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import sys
from lib import config
from lib.app_constants import AppConstants
from lib.constants import Constant
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['JOB_NAME','cfg_file_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

cfg_params = config.resolve_cfg_params(args)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dict_sources = {}
APP_LAYER_NAME = 'STAGE_ODS'
cfg_app = cfg_params[APP_LAYER_NAME]
src_sys = args['source_system'].lower()

for each_file in source_file_list:
    src_file_path = os.path.join(cfg_app['src_folder'], src_sys, each_file['src_file'])
    cntl_file_path = os.path.join(cfg_app['src_folder'], src_sys, each_file['cntl_file'])
    df_src = data_transform.load_delimited_file(sc, src_file_path, '\031', skip_cnt=1)

    df_src = df_src \
        .withColumn(AppConstants.system_effective_date_, lit(args[Constant.system_effective_date]))

    df_cntl = data_transform.load_delimited_file(sc, cntl_file_path, '\031', skip_cnt=1)
    df_src_with_admin_dt = data_transform.add_admin_date_frm_cntl(df_src, df_cntl)
    dict_sources[each_file['src_file'][:-5]] = df_src_with_admin_dt

if bool(dict_sources):
        for file_name, data in dict_sources.items():
            dict_sources[file_name] = data_transform.trim_and_null_blanks_Can_Inv(data)

if bool(dict_sources):
        for table_name, data in dict_sources.items():
            stg_file_path = os.path.join(cfg_app['tgt_folder'], table_name.lower())

            stg_file_path_incl_partition = os.path.join(cfg_app['tgt_folder'], table_name.lower(),
                                                        '{}={}'.format(AppConstants.system_effective_date_,
                                                                       args['system_effective_date']))
            file_operation.delete_s3_folder(stg_file_path_incl_partition)
            # file_operation.delete_s3_folder(stg_file_path)
            dynf = DynamicFrame.fromDF(data, glueContext, table_name.lower())
            glueContext.write_dynamic_frame_from_options(frame=dynf,
                                                         connection_type="s3",
                                                         connection_options={"path": stg_file_path,
                                                                             "partitionKeys": [
                                                                                 AppConstants.system_effective_date_]
                                                                             },
                                                         format="parquet",
                                                         format_options={},
                                                         transformation_ctx="")

job.commit()

