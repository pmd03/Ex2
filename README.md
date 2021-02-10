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


IDE used is PyCharm community 2020.3 

there are two files to implement besides above mentioned environment variables

DataExplorationEx2.py
db_properties.ini
runEx2.log (example of captured output of a run as standalone laptop using Windows 10)

run DataExplorationEx2.py from an IDE of choice ( python.exe DataExplorationEx2.py [probably can add absolute path for each file] )
DataExplorationEx2.py requires a hardcode change to set path of unzipped PostLinks.xml (which is got from https://archive.org/details/stackexchange
as stackoverflow.com-PostLinks.7z. I used this source file because it was one of the smaller size and my limited laptop resource.

Did not containerize - will try later.
