from pyspark.sql.types import Row
from utils.util import *

spark = getsparksession("metadata read")
print(spark)

path = "C:\\Users\\iramp\\PycharmProjects\\extention_validation\\resources\\appMetadata.json"

"metadata read"
metadataDf = readfile(spark,path,"json")
#metadataDf.printSchema()

#"data processing row process "
for row in metadataDf.collect():
    #print("row:",row)
    dis=getMetadataRead(row) #COLUMN VALUES SPLIT
    #print(dis.keys(), dis.values())
    srcDf = readfile(spark,dis["Source_file_path"],dis["Source_file_format"])
    srcDf.show(10)



# # Select the desired columns and apply future date validation
selected_columns = ["_c0","Date"]

# Filter rows where the date is greater than the current date
badrecords = srcDf.select("_c0","Date").where(col("Date") > current_date())

# Show the filtered DataFrame
badrecords.show(10)
