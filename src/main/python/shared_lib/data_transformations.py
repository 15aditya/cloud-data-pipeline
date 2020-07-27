
def trim_and_null_blanks(df):
    """
    """
    for dt in df.dtypes:
        if dt[1] == 'null':
            df = df.withColumn(dt[0], lit(None).cast(StringType()))
        else:
            df = df.withColumn(dt[0], when(trim(col(df[0])) == lit(''), lit(None).cast(StringType())) \
            .otherwise(trim(col(df[0]))))
            