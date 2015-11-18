import csv

def fix_score(s):
    if (s=="Not Available") or (s==""):
        return "-1"
    elif s=="Low (0 - 19,999 patients annually)":
        return "0"
    elif s=="Medium (20,000 - 39,999 patients annually)":
        return "20000"
    elif s=="High (40,000 - 59,999 patients annually)":
        return "40000"
    elif s=="Very High (60,000+ patients annually)":
        return "60000"
    else:
        return s
    
def fix_sample(s):
    if (s=="Not Available") or (s==""):
        return "-1"
    return s

# Clean Timely and Effective Care file
# Remove 'Not Available' and transform the Low, Medium, High and Very High
# values from the score field
file1 = open('effective_care.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],fix_score(row[11]),fix_sample(row[12]),row[13],row[14],row[15]]
    new_rows_list.append(new_row)
file1.close()

file2 = open('effective_care_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()

# Clean Timely and Effective Care - State file
# Remove 'Not Available' from score
file1 = open('effective_care_state.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],fix_score(row[4]),row[5],row[6],row[7]]
    new_rows_list.append(new_row)
file1.close()

file2 = open('effective_care_state_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()

# Clean Readmissions and Deaths file
# Remove 'Not Available'
file1 = open('readmissions.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],fix_score(row[12]),row[13],row[14],row[15],row[16],row[17]]
    new_rows_list.append(new_row)
file1.close()

file2 = open('readmissions_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()

# Clean hospital data.  This removes the quotes.
file1 = open('hospitals.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10]]
    new_rows_list.append(new_row)
file1.close()

file2 = open('hospitals_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()

# Clean meastures data.  This removes the quotes.
file1 = open('measure_dates.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],row[4],row[5]]
    new_rows_list.append(new_row)
file1.close()

file2 = open('measure_dates_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()

# Clean surveys data.  Remove 'Not Available' from scores and remove the quotes.
file1 = open('surveys_responses.csv', 'rb')
reader = csv.reader(file1)
new_rows_list = []
for row in reader:
    new_row = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9], \
        row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17], \
        row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26], \
        row[27],row[28],row[29],row[30],fix_score(row[31]),fix_score(row[32])]
    new_rows_list.append(new_row)
file1.close()

file2 = open('surveys_responses_clean.csv', 'wb')
writer = csv.writer(file2)
writer.writerows(new_rows_list)
file2.close()
