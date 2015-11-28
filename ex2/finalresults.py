import psycopg2
import sys

#create a connection to the database that holds the results
conn = psycopg2.connect(database="tweettest", user="postgres", password="test123", host="localhost", port="5432")

#create a cursor to use to perform actions on the database
cur = conn.cursor()

#Select
#if len(sys.argv) == 1:
if True:
    cur.execute("SELECT word, count FROM tweettest")
    records = cur.fetchall()
    wordlist = []
    for rec in records:
        wordlist.append(rec)
    conn.commit()
    wordlist.sort(key=lambda tup: tup[0])
    print wordlist
else:
    requestedWord = sys.argv[1]
#    requestedWord = "christmas"
    print requestedWord
    cur.execute("SELECT word, count FROM tweettest")
    records = cur.fetchall()
    count = 0
    for rec in records:
        word = rec[0]
        if word == requestedWord:
            count = rec[1]
#            break
    conn.commit()
    print "Total number of occurences of \"" + requestedWord + "\": " + str(count) + "\n"

conn.close()
