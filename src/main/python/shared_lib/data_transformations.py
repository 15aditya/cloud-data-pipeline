from pyspark.sql.functions import lit, col, trim, when
from pyspark.sql.types import StringType

def trim_and_null_blanks(df):
    """
    """
    for dt in df.dtypes:
        if dt[1] == 'null':
            df = df.withColumn(dt[0], lit(None).cast(StringType()))
        else:
            df = df.withColumn(dt[0], when(trim(col(df[0])) == lit(''), lit(None).cast(StringType())) \
            .otherwise(trim(col(df[0]))))
    
    return df


def load_delimited_file(sc, filepath, delimiter, skip_cnt=0):
    """
    """
    rdd = sc.textFile(filepath)
    splitted_rdd = rdd.map(lambda x: x.split(delimiter))
    schema = splitted_rdd.first()
    str_rdd = splitted_rdd.zipWithIndex().filter(lambda row: row[1] > (skip_cnt -1)).map(lambda row: row[0])
    df = str_rdd.toDF(schema)
    return df
    