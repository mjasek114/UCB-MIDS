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

result = sqlContext.sql('SELECT COUNT(SCORE), AVG(CAST(SCORE AS INT)) AS A1, MEASURENAME FROM hospital_procedure GROUP BY MEASURENAME ORDER BY A1 DESC LIMIT 10')
result.show()

#result = sqlContext.sql('SELECT COUNT(SCORE), AVG(CAST(SCORE AS INT)), STDDEV(CAST(SCORE AS INT)) AS A1, MEASURENAME FROM hospital_procedure GROUP BY MEASURENAME ORDER BY A1 DESC LIMIT 10')
#result.show()
