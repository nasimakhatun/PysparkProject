import sys
import uuid
from lib import DataLoader,ConfigLoader,Utils,Transformation
from lib.logger import Log4j

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: PysparkProject {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)
    job_run_env = sys.argv[1].upper()
    print(job_run_env)
    load_date = sys.argv[2]
    print(load_date)
    job_run_id="PysparkProject-" +str(uuid.uuid4())

    print("Initializing PysparkProject Job in " + job_run_env + "Job ID " + job_run_id)

    conf=ConfigLoader.get_config(job_run_env)

    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.info("Finished creating Spark Session")

    logger.info("Started Reading dataset_one df")
    df_1=DataLoader.read_datasetOne(spark,job_run_env)

    logger.info("Started Reading dataset_two df")
    df_2=DataLoader.read_datasettwo(spark,job_run_env)

    logger.info("Join dataset_one and dataset_two")
    df_1_2=Transformation.join_datasetone_two(df_1,df_2)

    logger.info("Rename Columns for Final DF")
    df_final=Transformation.get_finaldf(df_1_2)

    logger.info("Initiating writing ")
    DataLoader.write_finaldf(df_final,job_run_env)

    logger.info("Finished: Data Loaded Successfully")