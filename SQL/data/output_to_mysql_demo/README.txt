1. Spark takes use of Java JDBC API to output a DataFrame to any RDBs that support JDBC drivers. 
2. Under the hood, it requires a corresponding JDBC driver for the target RDB (f.g. MySQL)
3. Simply put the JDBC driver for a RDB (f.g. MySQL) in the Spark's environment. To do it, there are many ways of doing it. Putting the JDBC driver in jars folder in Spark's installation folder is one of them. Remember that in the multi-node cluster, all spark nodes need such driver under the jars folder because Spark programming is running on multiple nodes at the same time. 



cd ~/Desktop/spark_sql_101/output_to_mysql_demo/mysql_jdbc_connector/
cp mysql-connector-java-5.1.46.jar ~/spark-2.4.5-bin-hadoop2.7/jars/