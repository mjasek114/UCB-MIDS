#!/bin/bash

# install R and RStudio and start RStudio server
yum -y install R
yum -y install r-studio
wget https://download2.rstudio.org/rstudio-server-rhel-0.99.489-x86_64.rpm
yum -y install --nogpgcheck rstudio-server-rhel-0.99.489-x86_64.rpm
sudo rstudio-server stop
sudo rstudio-server start

#install streamparse
sudo yum install python27-devel -y
mv /usr/bin/python /usr/bin/python266
ln -s /usr/bin/python2.7 /usr/bin/python
sudo curl -o ez_setup.py https://bootstrap.pypa.io/ez_setup.py
sudo python ez_setup.py
sudo /usr/bin/easy_install-2.7 pip
sudo pip install virtualenv
wget --directory-prefix=/usr/bin/ https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
chmod a+x /usr/bin/lein
sudo /usr/bin/lein
pip install streamparse

# install psycopg2
pip install psycopg2

# install tweepy
pip install tweepy

# create a new database call tcount and a new table called tweetwordcount
psql -U postgres -c 'DROP DATABASE tcount'
psql -U postgres -c 'CREATE DATABASE tcount'
psql -U postgres -d tcount -c 'DROP TABLE tweetwordcount'
psql -U postgres -d tcount -c 'CREATE TABLE tweetwordcount (word TEXT PRIMARY KEY NOT NULL, count INT NOT NULL)'
