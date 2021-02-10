# Ex2
Starting from scratch i.e. downloads and installs all open source....

Task: ETL with pySpark 3.0.1

Windows 10 (for example: PS Get-ChildItem Env: | sort name)
SPARK_HOME=C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7
JAVA_HOME=C:\Java\jdk-15.0.1
HADOOP_HOME=C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\hadoop

All jars were pip installed to C:\Users\username\Spark\spark-3.0.1-bin-hadoop2.7\jars
for example
postgresql-42.2.18.jar
spark-xml_2.12-0.9.0

IDE used was PyCharm community 2020.3 

there are three files to deploy/implement besides above mentioned environment variables checking/setting.

1. db_properties.ini
2. createTable_Postlink.sql
3. DataExplorationEx2.py

Note runEx2.log (is example of captured output of a run as standalone laptop using Windows 10)

Run DataExplorationEx2.py from an IDE of choice ( python.exe DataExplorationEx2.py [probably can add absolute path for each file] )
DataExplorationEx2.py requires a hardcode change in main dunder conditional section(confPath and xmlPath) to set absolute path of ini file and the unzipped PostLinks.xml (which is got from https://archive.org/details/stackexchange named stackoverflow.com-PostLinks.7z. Used this source file because it was one of the smaller size and a limited laptop standalone resource.
The db_properties.ini file should be editied with appropriate values before running DataExplorationEx2.py.

Logon as user postgres create a database if required, run the createTable_Postlink.sql script to create an empty database table.

NB Cautionary note whichever tablename used, will be overwritten which means in postgresql if it already exists it will be dropped and recreated, any data in this table beforehand will be lost. The tablename you decide to use is important make sure it is a new tablename for saftey :)

Did not containerize - will try later. Logging/monitoring pipeline results in persistent table(s) TBC next.
