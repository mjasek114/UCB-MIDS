from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)


# Create Hospital Procedure State Table
linesHPSRDD = sc.textFile('file:///data/w205/exercise1/data/effective_care_state_clean.csv')
partsHPSRDD = linesHPSRDD.map(lambda l: l.split(','))
HPSRDD = partsHPSRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7]))
schemaStringHPS = 'STATE CONDITION MEASURENAME MEASUREID SCORE FOOTNOTE MEASURESTARTDATE MEASUREENDDATE'
fieldsHPS = [StructField(field_name, StringType(), True) for field_name in schemaStringHPS.split()]
schemaHPS = StructType(fieldsHPS)
schemaHPSData = sqlContext.createDataFrame(HPSRDD, schemaHPS)
schemaHPSData.registerTempTable('hospital_procedure_state')
result = sqlContext.sql('SELECT COUNT(SCORE), AVG(CAST(SCORE AS INT)), MAX(CAST(SCORE AS INT)) AS A1, MEASURENAME FROM hospital_procedure_state GROUP BY MEASURENAME ORDER BY A1 DESC LIMIT 10')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="AMI_10" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="AMI_7a" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE (MEASUREID="OP_21") AND (CAST(SCORE AS INT) != -1) ORDER BY A1 LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="HF_2" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="IMM_2" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="OP_23" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="PN_6" ORDER BY A1 DESC LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE (MEASUREID="STK_1") AND (CAST(SCORE AS INT) != -1) ORDER BY A1 LIMIT 20')
result.show()
result = sqlContext.sql('SELECT CAST(SCORE AS INT) AS A1, STATE FROM hospital_procedure_state WHERE MEASUREID="VTE_1" ORDER BY A1 DESC LIMIT 20')
result.show()


