from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
#        self.redis = StrictRedis()
        self.conn = psycopg2.connect(database="Tcount", user="postgres", password="pass", host="localhost", port="5432")

        #Create a Table
        #The first step is to create a cursor. 
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE Tweetwordcount
            (word TEXT PRIMARY KEY     NOT NULL,
            count INT     NOT NULL);''')
        self.conn.commit()

    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        

        # Increment the local count
        self.counts[word] += 1
        count = self.counts[word]
        self.emit([word, count])

        cur = self.conn.cursor()
        if count==1:
            #Insert
            cur.execute("INSERT INTO Tweetwordcount (word,count) \
                VALUES (word, 1)");
        else:
            #Update
            #Assuming you are passing the tuple (uWord, uCount) as an argument
            cur.execute("UPDATE Tweetwordcount SET count=%s WHERE word=%s", (count, word))
        self.conn.commit()

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))
