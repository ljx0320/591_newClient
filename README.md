Environment and necessary tools: Linux 18.04, java 1.8, maven, protobuf 2.5(must be this version)

In addition to this repo, you should also download the hadoop source code: https://github.com/ljx0320/hadoop_modified

Before you do the following steps, make sure you have done https://github.com/ljx0320/hadoop_modified/blob/master/README.txt
Step 1: install the hadoop-hdfs-2.10.0.jar and hadoop-common-2.10.0.jar by using maven:
mvn install:install-file -Dfile=/path/to/jar-file/xxx.jar -DgroupId=org.apache.hadoop -DartifactId=hadoop-hdfs -Dversion=2.10.0 -Dpackaging=jar -DgeneratePom=true

Step 2: compile and package the newClient:
mvn clean package

Step 3: before you want to send messages to name node of hadoop, run the newClient up:
java -jar target/hadoop-newclient-2.10.0.jar

Step 4: Now you can send messages! Refer to https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-common/FileSystemShell.html
