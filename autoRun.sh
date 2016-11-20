echo -e "\n >>>>>>>>>>>>>>>>>>>>>>>Start Clearing HDFS<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
hdfs dfs -rm -r /*input* /*output* /tmp /*History*
hdfs dfs -ls /

echo -e "\n >>>>>>>>>>>>>>>>>>>>>>>Start PreProcessing<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
javac PreProcess.java
java PreProcess watchingHistory.txt extendedWatchingHistory.txt
ls -al | grep "extendedWatchingHistory.txt"

echo -e "\n >>>>>>>>>>>>>>>>>>>>>>>Start Loading HDFS<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
hdfs dfs -mkdir /watchingHistory
hdfs dfs -put watchingHistory.txt /watchingHistory

hdfs dfs -mkdir /extendedWatchingHistory
hdfs dfs -put extendedWatchingHistory.txt /extendedWatchingHistory

hdfs dfs -ls /

echo -e "\n >>>>>>>>>>>>>>>>>>>>>>>Start Executing MapReduce<<<<<<<<<<<<<<<<<<<<<<<<<<<<"

hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class

START=$(date +%s)
hadoop jar recommender.jar Driver /watchingHistory /extendedWatchingHistory /output 2
END=$(date +%s)
DIFF=$(( $END - $START ))
echo "*******************************************************************************"
echo "************** MapReducer Jobs elapsed time $DIFF seconds *********************"
echo "*******************************************************************************"

hdfs dfs -ls /
hdfs dfs -cat /output/*

