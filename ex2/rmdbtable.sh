#!/bin/bash

# create a new database call tcount and a new table called tweetwordcount
psql -U postgres -c 'DROP DATABASE tcount'
psql -U postgres -c 'CREATE DATABASE tcount'
psql -U postgres -d tcount -c 'DROP TABLE tweetwordcount'
psql -U postgres -d tcount -c 'CREATE TABLE tweetwordcount (word TEXT PRIMARY KEY NOT NULL, count INT NOT NULL)'
