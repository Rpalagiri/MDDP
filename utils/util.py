from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def getsparksession(appname):
    spark = SparkSession.builder.appName(appname).getOrCreate()
    return spark

# Function to read the input df:
def readfile(spark: object, filepath: object, fileFormat: object) -> object:
    #print(filepath)
    #print(fileFormat)
    if(fileFormat.lower()=="csv" or fileFormat.lower()=="txt"):
        df = spark.read.format(fileFormat).option("header","true").option("inferSchema","true").load(filepath)
    elif fileFormat.lower()=="json":
        df = spark.read.format(fileFormat).option("multiline", "true").load(filepath)
    else:
        df = spark.read.formatO(fileFormat).load(filepath)
    return df

#Function for writing the output df:
def writeFile(df,output_path, output_format, savemode):
   return df.write.format(output_format).mode(savemode).save(output_path)

#Function for date_validation_check in any df:
def datevalidation(df,current_date):
    current_date = current_date().alias("current_date")
    valid_dates = df.select(col("date"), current_date) \
        .withColumn("is_future_date", date_format(col("date"), "yyyy-MM-dd") > col("current_date")) \
        .drop("current_date")
    invalid_future_dates = df.filter(col("date") > current_date())
    # Display the results
    return valid_dates,invalid_future_dates
#############

def datevalidations(df, datecolumn):

    current_date_str = current_date().cast("string")
    rejected_df = df.filter(col(datecolumn) > current_date_str)
    accepted_df = df.filter(col(datecolumn) <= current_date_str)
    return rejected_df, accepted_df


def datevalidations(df,dates):
    # Add a new column to check if the date is in the future
    df_with_future_dates = df.withColumn("is_future_date", (current_date() < df.dates))

    # Save the future dates aside to a new DataFrame
    future_dates = df_with_future_dates.filter(df_with_future_dates.is_future_date)

    # Show the future dates
    future_dates.show()

    # You can save the future dates to a file or perform any further operations as needed
    # For example, to save as Parquet files:
    future_dates.write.mode("overwrite").parquet("/path/to/save/future_dates.parquet")



def getMetadataRead(row):
    dis={}
    dis["Source_file_path"] = row.Source_file_path
    dis["Source_file_format"] = row.Source_file_format
    dis["Select_col_list"] = row.Select_col_list

    return dis


def is_future_date(date):
    return date > current_date()