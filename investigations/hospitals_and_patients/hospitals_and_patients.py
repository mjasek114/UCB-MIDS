from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)

# Create Surveys Responses Table
linesSRRDD = sc.textFile('file:///data/w205/exercise1/data/surveys_responses_clean.csv')
partsSRRDD = linesSRRDD.map(lambda l: l.split(','))
SRRDD = partsSRRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17],p[18],p[19],p[20],p[21],p[22],p[23],p[24],p[25],p[26],p[27],p[28],p[29],p[30],p[31],p[32]))
schemaStringSR = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY COM1 COM2 COM3 COM4 COM5 COM6 RESP1 RESP2 RESP3 PAIN1 PAIN2 PAIN3 COM7 COM8 COM9 CLEAN1 CLEAN2 CLEAN3 DIS1 DIS2 DIS3 OVER1 OVER2 OVER3 HBASESCORE HCONSISTSCORE'
fieldsSR = [StructField(field_name, StringType(), True) for field_name in schemaStringSR.split()]
schemaSR = StructType(fieldsSR)
schemaSRData = sqlContext.createDataFrame(SRRDD, schemaSR)
schemaSRData.registerTempTable('surveys_responses')
result = sqlContext.sql('SELECT COUNT(HBASESCORE), AVG(CAST(HBASESCORE AS INT)+CAST(HCONSISTSCORE AS INT)) AS A1, HOSPITALNAME FROM surveys_responses GROUP BY HOSPITALNAME ORDER BY A1 DESC LIMIT 20')
result.show()

