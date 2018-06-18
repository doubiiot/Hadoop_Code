hdfs dfs -rm -r output
hdfs dfs -rm -r output2
hdfs dfs -rm -r output3
hdfs dfs -rm -r output4
hdfs dfs -rm -r input
hdfs dfs -mkdir input
hdfs dfs -chmod 777 input

javac WordCount.java 
jar -cvf WordCount.jar ./*.class
hadoop jar WordCount.jar WordCount  hdfs://localhost:9000/user/doubi/input  hdfs://localhost:9000/user/doubi/output ~/Desktop/score

#
javac WordCount2.java 
jar -cvf WordCount2.jar ./*.class
hadoop jar WordCount2.jar WordCount2  hdfs://localhost:9000/user/doubi/input  hdfs://localhost:9000/user/doubi/output ~/Desktop/score


hdfs dfs -cat output/*
hdfs dfs -cat output2/*
hdfs dfs -cat output3/*
hdfs dfs -cat output4/*



