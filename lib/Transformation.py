from pyspark.sql.functions import struct, lit, col, array, when, isnull, filter, current_timestamp, date_format, expr, \
    collect_list


def get_finaldf(df):
    return df.selectExpr("cast(id as int) as client_identifier",
                     "email as mail_id",
                     "btc_a as bitcoin_address",
                     "cc_t as credit_card_type"
                     )


def join_datasetone_two(df1, df2):
    return df1.join(df2, "id", "left") \
        .select(df1.id,df1.email,df2.btc_a,df2.cc_t)
 


  