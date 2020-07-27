"""
Description:
    This program reads data from source area (S3) and loads it into staging area (S3)
"""
import os
from awsglue.dynamicframe import DynamicFrame
from shared_lib import file_operation, data_transformations
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import sys
from shared_lib import config
from pyspark.sql.functions import lit

args = getResolvedOptions(sys.argv, ['JOB_NAME','cfg_file_path', 'system_effective_date'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

cfg_params = config.resolve_cfg_params(args)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

src_file_path = os.path.join(cfg_params['src_folder'], cfg_params['src_file'])
df_src = data_transformations.load_delimited_file(sc, src_file_path, ',', skip_cnt=1)

df_src = df_src.withColumn('system_effective_date_', lit(args['system_effective_date']))

df_tgt =  data_transformations.trim_and_null_blanks_Can_Inv(df_src)

stg_file_path = os.path.join(cfg_params['tgt_folder'], cfg_params['tgt_file'])

stg_file_path_incl_partition = os.path.join(cfg_params['tgt_folder'], cfg_params['tgt_file'], 
'{}={}'.format('system_effective_date_', args['system_effective_date']))
file_operation.delete_folder(stg_file_path_incl_partition)

dynf = DynamicFrame.fromDF(df_tgt, glueContext, cfg_params['tgt_file'])
glueContext.write_dynamic_frame_from_options(frame=dynf,
                                                connection_type="s3",
                                                connection_options={"path": stg_file_path,
                                                                        "partitionKeys": [
                                                                                 'system_effective_date_']
                                                                             },
                                                         format="parquet",
                                                         format_options={},
                                                         transformation_ctx="")

job.commit()

