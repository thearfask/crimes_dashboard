# Crimes Analysis in Chicago for 2001 to Present

Multiple variants of this project is made but this will be the final version with all latest things to be included.
  
	1) Used Apache Spark(PySpark) to write the core logic to process the data and save to MYSQL server. 
	2) Apache Superset reads tables from MYSQL server and provides required reports.
	3) MYSQL Server - EC2 micro free tier instance that holds the data.
	4) Superset - EC2 micro free tier instance = Installed Superset on a free instance to display the Dashboard
	5) Apache Spark - Code execution done from Local Machine.

# Project Details

https://data.cityofchicago.org/ - Provides monthly update on crimes that were observed in Chicago. This Public dataset helps in finding the crimes by Count, Year, Type etc.

For this project I have used a complete DataSet which is around 1.69 GB in size and is a .TSV file.

Features Added:
	
	1) Use Apache Spark to process the records.
	2) Add transformation and seperate desired logic to Data-frames.
	3) Each DF will be a Table for MYSQL db -> db_crimes_db (Which is an EC2 instance)
	4) After processing the data save to MYSQL Database to respective tables.
	
	
Features ToBe Added:

	1) Save cleaned _raw data to HDFS for reporting.
	2) Adding HDFS to Elastic Search and Kibana for Learning purpose.
	
# Reporting Tool - Apache Superset

	1) Used the simple and easy to use Apache Superset tool for Visualization.

![alt text](https://github.com/thearfask/crimes_dashboard/blob/main/screenshot/Screenshot%202020-10-11%20at%202.27.33%20AM.png)
