from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
#        self.redis = StrictRedis()
#        self.conn = psycopg2.connect(database="tcount", user="postgres", password="", host="localhost", port="5432")

    def process(self, tup):
        curWord = tup.values[0]
#        print(curWord)
#        print(type(curWord))
        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        

        # Increment the local count
        self.counts[curWord] += 1
        count = self.counts[curWord]
        print(curWord + ": " + str(count))
        self.emit([curWord, count])

        conn = psycopg2.connect(database="tcount", user="postgres", password="", host="localhost", port="5432")
        cur = conn.cursor()
        if count==1:
            #Insert
            cur.execute("INSERT INTO tweetwordcount (word,count) VALUES (%s, 1)", (curWord,))
        else:
            #Update
            #Assuming you are passing the tuple (uCount, uWord) as an argument
            cur.execute("UPDATE tweetwordcount SET count=%s WHERE word=%s", (count, str(curWord)))
        conn.commit()
        conn.close()

        # Log the count - just to see the topology running
        self.log('%s: %d' % (curWord, self.counts[curWord]))
