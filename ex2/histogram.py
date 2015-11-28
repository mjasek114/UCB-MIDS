import psycopg2
import sys

#create a connection to the database that holds the results
conn = psycopg2.connect(database="tcount", user="postgres", password="", host="localhost", port="5432")

#create a cursor to use to perform actions on the database
cur = conn.cursor()

#Select
if len(sys.argv) == 1:
    cur.execute("SELECT word, count FROM tweetwordcount")
    records = cur.fetchall()
    wordlist = []
    for rec in records:
        word = rec[0]
        count = rec[1]
        wordlist.append(rec)
    conn.commit()
    wordlist.sort(key=lambda tup: tup[1], reverse=True)
    for elem in wordlist:
        word = elem[0]
        count = elem[1]
        print word + ": " + str(count)
elif len(sys.argv) == 2:
    #?? could fix to reject invalid args
    args = sys.argv[1].strip(',').split(',')
    lowerBound = float(args[0])
    upperBoundThere = False
    if len(args) > 1:
        upperBound = float(args[1])
        upperBoundThere = True
    cur.execute("SELECT word, count FROM tweetwordcount")
    records = cur.fetchall()
    wordlist = []
    for rec in records:
        word = rec[0]
        count = rec[1]
        if (count >= lowerBound):
            if (upperBoundThere == True):
                if count <= upperBound:
                    wordlist.append(rec)
            else:
                wordlist.append(rec)
    conn.commit()
    wordlist.sort(key=lambda tup: tup[1], reverse=True)
    for elem in wordlist:
        word = elem[0]
        count = elem[1]
        print word + ": " + str(count)
else:
    #?? could fix it to accepts 25,54 100 as arguments and reject invalid args
    lowerBound = float(sys.argv[1].strip(','))
    upperBound = float(sys.argv[2].strip(','))
    cur.execute("SELECT word, count FROM tweetwordcount")
    records = cur.fetchall()
    wordlist = []
    for rec in records:
        word = rec[0]
        count = rec[1]
        if (count >= lowerBound and count <= upperBound):
            wordlist.append(rec)
    conn.commit()
    wordlist.sort(key=lambda tup: tup[1], reverse=True)
    for elem in wordlist:
        word = elem[0]
        count = elem[1]
        print word + ": " + str(count)

conn.close()
