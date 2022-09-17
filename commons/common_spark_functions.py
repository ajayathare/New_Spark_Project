# Import Packages

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create Spark Session
def Create_spark_Session(Application_name, Master_Name):
    spark = SparkSession \
          .builder \
          .master(Master_Name) \
          .appName(Application_name) \
          .getOrCreate()

    return spark

# Function to Create data Frame
def Read_Data (Spark_Session,File_Type,Schema,Header,Deli,File_loc):
    df = Spark_Session.read.format(File_Type) \
        .option("inferSchema", Schema) \
        .option("header", Header) \
        .option("sep", Deli) \
        .load(File_loc)

    return df

# Function to Create Temp View
def Create_Temp_views (Data_Frame, Table_Name):
    view = Data_Frame.createOrReplaceTempView (Table_Name)

    return view

# Function to Read Temp View
def Read_Temp_View (spark_session,Second_name_query):
    Temp = spark_session.sql(Second_name_query)

    return Temp