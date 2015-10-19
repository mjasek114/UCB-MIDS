from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)

# Create Hospital Procedure Table
linesHPRDD = sc.textFile('file:///data/w205/exercise1/data/effective_care_clean.csv')
partsHPRDD = linesHPRDD.map(lambda l: l.split(','))
HPRDD = partsHPRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15]))
schemaStringHP = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY PHONE CONDITION MEASUREID MEASURENAME SCORE SAMPLE FOOTNOTE MEASURESTARTDATE MEASUREENDDATE'
fieldsHP = [StructField(field_name, StringType(), True) for field_name in schemaStringHP.split()]
schemaHP = StructType(fieldsHP)
schemaHPData = sqlContext.createDataFrame(HPRDD, schemaHP)
schemaHPData.registerTempTable('hospital_procedure')

result = sqlContext.sql('SELECT COUNT(SCORE), AVG(CAST(SCORE AS INT)), MAX(CAST(SCORE AS INT)) AS A1, MEASURENAME FROM hospital_procedure GROUP BY MEASURENAME ORDER BY A1 DESC LIMIT 10')
result.show()


result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="AMI_10" ORDER BY A1 DESC LIMIT\
 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE (MEASUREID="AMI_7a") AND (CAST(SCORE AS INT) != -1) ORDER BY A1 DESC LIMIT\
 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE (MEASUREID="OP_21") AND (CAST(SCORE AS IN\
T) != -1) ORDER BY A1 LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="HF_2" ORDER BY A1 DESC LIMIT 2\
0')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="IMM_2" ORDER BY A1 DESC LIMIT \
20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="OP_23" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="PN_6" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE (MEASUREID="STK_1") AND (CAST(SCORE AS INT) != -1)\
 ORDER BY A1 LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, PROVIDERID FROM hospital_procedure WHERE MEASUREID="VTE_1" ORDER BY A1 DESC LIMIT 20')
result.show()

# Create Hospital Procedure State Table
#linesHPSRDD = sc.textFile('file:///data/w205/exercise1/data/effective_care_state_clean.csv')
#partsHPSRDD = linesHPSRDD.map(lambda l: l.split(','))
#HPSRDD = partsHPSRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7]))
#schemaStringHPS = 'STATE CONDITION MEASURENAME MEASUREID SCORE FOOTNOTE MEASURESTARTDATE MEASUREENDDATE'
#fieldsHPS = [StructField(field_name, StringType(), True) for field_name in schemaStringHPS.split()]
#schemaHPS = StructType(fieldsHPS)
#schemaHPSData = sqlContext.createDataFrame(HPSRDD, schemaHPS)
#schemaHPSData.registerTempTable('hospital_procedure_state')
#resultsHPS = sqlContext.sql('SELECT SCORE FROM hospital_procedure_state GROUP BY STATE ORDER BY STATE DESC LIMIT 10')
#result = sqlContext.sql('SELECT COUNT(SCORE), AVG(CAST(SCORE AS INT)), MAX(CAST(SCORE AS INT)) AS A1, MEASURENAME FROM hospital_procedure_state GROUP BY MEASURENAME ORDER BY A1 DESC LIMIT 10')
#result.show()


