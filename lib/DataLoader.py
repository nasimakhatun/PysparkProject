from lib import ConfigLoader

def get_datasetOne_schema():
    schema = """id int,first_name string,last_name string,
        email string,country string"""
    return schema


def get_datasetTwo_schema():
    schema = """id int,btc_a string,cc_t string,
    cc_n long"""
    return schema


def read_datasetOne(spark, env):
    country_filter = ConfigLoader.get_data_filter(env, "country.filter")
    country_filter_values = ','.join([f"'{country}'" for country in country_filter.split(',')])
    print(country_filter_values)
    path1=ConfigLoader.get_path(env, "datasetone.path")
    return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_datasetOne_schema()) \
            .load(path1) \
            .where(f"country in ({country_filter_values})")


def read_datasettwo(spark, env):
    path1=ConfigLoader.get_path(env, "datasettwo.path")
    return spark.read \
            .format("csv") \
            .option("header", "true") \
            .schema(get_datasetTwo_schema()) \
            .load(path1)

def write_finaldf(df,env):
    path1=ConfigLoader.get_path(env, "finaldataset.path")
    return df.write.format("csv").mode("overwrite").option("header","true").save(path1)
