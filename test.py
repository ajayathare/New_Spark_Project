from commons.common_spark_functions import *

from queries.test_queries import *
from commons import global_config
from commons.global_config import *

global_config.my_file_type = "json"
df = Read_CSV_Data(spark_session,file_type,infer_schema,first_row_is_header,delimiter,file_location)
# df.show(2)
df.createOrReplaceTempView("df_table")
# createTempView(df,"df_table",spark)
spark.sql(first_name_query)
# runSQL(query,spark)

""""
1. Implement other common and common spark functions
2. create csv in resources dir and try to access it from your code
3. create commonn func to read any type of input file using generic functionality
4. make use of default arguments in creating function ( for step 3)
5. move SS.py to common spark functions or improvise it
"""


