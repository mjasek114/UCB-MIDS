from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
sc = SparkContext("local", "weblog app")
sqlContext = SQLContext(sc)

# Create Hospital Table
linesHRDD = sc.textFile('file:///data/w205/exercise1/data/hospitals_clean.csv')
partsHRDD = linesHRDD.map(lambda l: l.split(','))
HRDD = partsHRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10]))
schemaStringH = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY PHONE HOSPITALTYPE HOSPITALOWN EMERGENCYSRV'
fieldsH = [StructField(field_name, StringType(), True) for field_name in schemaStringH.split()]
schemaH = StructType(fieldsH)
schemaHData = sqlContext.createDataFrame(HRDD, schemaH)
schemaHData.registerTempTable('hospital_all')
HDF = sqlContext.sql('SELECT PROVIDERID, HOSPITALNAME, STATE FROM hospital_all')
HDF.registerTempTable('hospital')
result = sqlContext.sql('SELECT * FROM hospital')
result.show()

# Create Hospital Procedure Table
linesHPRDD = sc.textFile('file:///data/w205/exercise1/data/effective_care_clean.csv')
partsHPRDD = linesHPRDD.map(lambda l: l.split(','))
HPRDD = partsHPRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15]))
schemaStringHP = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY PHONE CONDITION MEASUREID MEASURENAME SCORE SAMPLE FOOTNOTE MEASURESTARTDA\
TE MEASUREENDDATE'
fieldsHP = [StructField(field_name, StringType(), True) for field_name in schemaStringHP.split()]
schemaHP = StructType(fieldsHP)
schemaHPData = sqlContext.createDataFrame(HPRDD, schemaHP)
schemaHPData.registerTempTable('hospital_procedure_all')
HPdf = sqlContext.sql('SELECT PROVIDERID, MEASUREID, SCORE FROM hospital_procedure_all')
HPdf.registerTempTable('hospital_procedure')
result = sqlContext.sql('SELECT * FROM hospital_procedure')
result.show()

# Create Hospital Procedure State Table
linesHPSRDD = sc.textFile('file:///data/w205/exercise1/data/effective_care_state_clean.csv')
partsHPSRDD = linesHPSRDD.map(lambda l: l.split(','))
HPSRDD = partsHPSRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7]))
schemaStringHPS = 'STATE CONDITION MEASURENAME MEASUREID SCORE FOOTNOTE MEASURESTARTDATE MEASUREENDDATE'
fieldsHPS = [StructField(field_name, StringType(), True) for field_name in schemaStringHPS.split()]
schemaHPS = StructType(fieldsHPS)
schemaHPSData = sqlContext.createDataFrame(HPSRDD, schemaHPS)
schemaHPSData.registerTempTable('hospital_procedure_state')

# Create Readmissions Table
linesRRDD = sc.textFile('file:///data/w205/exercise1/data/readmissions_clean.csv')
partsRRDD = linesRRDD.map(lambda l: l.split(','))
RRDD = partsRRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17]))
schemaStringR = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY PHONE MEASURENAME MEASUREID COMPNATL DEMONINATOR SCORE LOWEREST HIGHEREST FOOTNOTE MEASURESTARTDA\
TE MEASUREENDDATE'
fieldsR = [StructField(field_name, StringType(), True) for field_name in schemaStringR.split()]
schemaR = StructType(fieldsR)
schemaRData = sqlContext.createDataFrame(RRDD, schemaR)
schemaRData.registerTempTable('readmissions')

# Create Surveys Responses Table
linesSRRDD = sc.textFile('file:///data/w205/exercise1/data/surveys_responses_clean.csv')
partsSRRDD = linesSRRDD.map(lambda l: l.split(','))
SRRDD = partsSRRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17],p[18],p[19],p[20],p[21],p[22],p[23],p[24],p[25],p[26],p[27],p[28],p[29],p[30],p[31],p[32]))
schemaStringSR = 'PROVIDERID HOSPITALNAME ADDRESS CITY STATE ZIP COUNTY COM1 COM2 COM3 COM4 COM5 COM6 RESP1 RESP2 RESP3 PAIN1 PAIN2 PAIN3 COM7 COM8 COM9 CLEAN1 CLEAN2 CLEAN3 DIS1 DIS2 DIS3 OVER1 OVER2 OVER3 HBASESCORE HCONSISTSCORE'
fieldsSR = [StructField(field_name, StringType(), True) for field_name in schemaStringSR.split()]
schemaSR = StructType(fieldsSR)
schemaSRData = sqlContext.createDataFrame(SRRDD, schemaSR)
schemaSRData.registerTempTable('surveys_responses_all')
SRdf = sqlContext.sql('SELECT PROVIDERID, HBASESCORE, HCONSISTSCORE FROM surveys_responses_all')
SRdf.registerTempTable('surveys_responses')
result = sqlContext.sql('SELECT * FROM surveys_responses')
result.show()

# Create Procedure Table
linesPRDD = sc.textFile('file:///data/w205/exercise1/data/measure_dates_clean.csv')
partsPRDD = linesPRDD.map(lambda l: l.split(','))
PRDD = partsPRDD.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5]))
schemaStringP = 'MEASURENAME MEASUREID MEASURESTARTQTR MEASUREENDQTR MEASURESTARTDATE MEASUREENDDATE'
fieldsP = [StructField(field_name, StringType(), True) for field_name in schemaStringP.split()]
schemaP = StructType(fieldsP)
schemaPData = sqlContext.createDataFrame(PRDD, schemaP)
schemaPData.registerTempTable('procedure_all')
Pdf = sqlContext.sql('SELECT MEASURENAME, MEASUREID FROM procedure_all')
Pdf.registerTempTable('procedure')
results = sqlContext.sql('SELECT * FROM procedure')
results.show()
