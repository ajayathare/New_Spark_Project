from commons.global_config import *
from commons.common_spark_functions import *
from commons.global_config import *
from queries.test_queries import *
import logging


logger = logging.getLogger("Sample logging")
try:
    logger.warn("Spark Session Creation Started")
    spark_session = Create_spark_Session(app_name, master)

    logger.warn("Data Frame Creation Started")
    df = Read_Data(spark_session,file_type,infer_schema,first_row_is_header,delimiter,file_location)

    df.show(7)

    logger.warn("Tem View Creation Started")
    Create_Temp_views(df,Table)

    logger.warn("Temp View Reading Started")
    df_view = Read_Temp_View(spark_session,Second_name_query)

    df_view.show()

    logger.warn("Dataframe Writing Started")
    df_view.write.csv("C:\\Users\\hp\\Desktop\\New_Spark Project\\OutPut\\Emp1\\")

    logger.warn("All Operation Done")

    ''' df.write.option("header",True) \
    .option("delimiter","|") \
    .option("compression", "gzip") \
    .format("csv") \
    .save(Path1) '''

    ''' Path1 =  /tmp/park_output/datacsv
    path2 = hdfs://nn1home:8020/csvfile
    PAth3 = s3a://sparkbyexamples/csv/datacsv '''


    #df_view.write.parquet("path")
    #df_view.coalesce(1).write.csv("path")
    # 200 partitions - 200 output files
    # create_output_file(df, format, delimiter, path, spark_session)
    # create common spark func to save df as csv, json, parquet
    # create_tsv()
    # create another common spark func to save df as tab delimited txt
    # 4 output files -- create var with base loc -- c:\Desktop\Ajay\
    # if you want to save df1, it should be like ( use python filesystem cmds)
    # c:\Desktop\Ajay\df1.csv, c:\Desktop\Ajay\df2.csv (part-0000-aksjdhakd.csv)

    # app1(output -- parquet) --> app2(output -- parquet) -- >
    # app3(csv, txt, json) --> dashboard(tableau, qlikview,, tibco spotfire)

except:
    logger.exception("Failed abruptly")
