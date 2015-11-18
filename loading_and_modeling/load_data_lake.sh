mv "Hospital General Information.csv" hospitals.header.csv
mv "Timely and Effective Care - Hospital.csv" effective_care.header.csv
mv "Timely and Effective Care - State.csv" effective_care_state.header.csv
mv "Readmissions and Deaths - Hospital.csv" readmissions.header.csv
mv "Measure Dates.csv" measure_dates.header.csv
mv hvbp_hcahps_05_28_2015.csv surveys_responses.header.csv

tail -n +2 hospitals.no_header.csv > hospitals.csv
tail -n +2 effective_care.header.csv > effective_care.csv
tail -n +2 effective_care_state.header.csv > effective_care_state.csv
tail -n +2 readmissions.header.csv > readmissions.csv
tail -n +2 measure_dates.header.csv > measure_dates.csv
tail -n +2 surveys_responses.header.csv > surveys_responses.csv

sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/hospitals.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/effective_care.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/effective_care_state.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/readmissions.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/measure_dates.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -put /home/w205/exercise1/data/surveys_responses.csv /user/w205/hospital_compare
sudo -u hdfs hdfs dfs -ls /user/w205/hospital_compare