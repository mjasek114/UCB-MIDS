1.  Install the ucbw205_complete_plus_postgres_PY2.7 with the easybutton
instructions from the file:  TheEasyButtonforYourAWSEnvironment_2_2.pdf

Make sure that these Inbound rules are in your security group
Type 	       	     Protocol   Port Range     Source
HTTP	  	     TCP 	80	       0.0.0.0/0
Custom TCP Rule	     TCP	4040	       0.0.0.0/0
Custom TCP Rule	     TCP	50070	       0.0.0.0/0
Custom TCP Rule	     TCP	8080	       0.0.0.0/0
PostgreSQL 	     TCP 	5432	       0.0.0.0/0
SSH	   	     TCP	22	       0.0.0.0/0
Custom TCP Rule	     TCP	10000  	       0.0.0.0/0
Custom TCP Rule	     TCP	8787  	       0.0.0.0/0
Custom TCP Rule	     TCP	8020  	       0.0.0.0/0
Custom TCP Rule	     TCP	3838  	       0.0.0.0/0
MYSQL/Aurora	     TCP	3306  	       0.0.0.0/0
Custom TCP Rule	     TCP	8088  	       0.0.0.0/0
HTTPS  	   	     TCP	443    	       0.0.0.0/0

2. Change to the /data directory with the command
cd /data

3.  Make sure that the Postgres server is running.  If it's not start it 
with this command
./start_postgres.sh

4.  Clone my UCB-MIDS repository from github with this command
git clone https://mjasek114@github.com/mjasek114/UCB-MIDS

5.  Change to the UCB-MIDS directory with this command
cd UCB-MIDS

6.  Pull my exercise 2 directory with this command
git pull origin hw_dev

7.  Change to the ex2 directory with this command
cd ex2

8.  Run the install script to install my application
./install-script.sh

   8a. when you see this warning
      WARNING: You're currently running as root; probably by accident.
      Press control-C to abort or Enter to continue as root.
      Set LEIN_ROOT to disable this warning.

   8b. Press Enter to continue

9.  Change to the ex2 directory with this command
cd EX2Tweetwordcount/

10. Type this command to run my applications
sparse run

   10a.  Press Ctrl-z when you want to stop the application

11.  Move up 1 directory with this command
cd ..

12.  Run the serving files with these commands
   a. python finalresults.py hello
   b. python finalresults.py
   c. python histogram.py 3, 8

13.  The requested screenshots are in the screenshots directory

14.  The Architecture.pdf file is in the ex2 directory

15.  The Plot.png file is in the ex2 directory
