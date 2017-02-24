rm ProblemOne*.class
hadoop com.sun.tools.javac.Main ProblemOne.java
jar cf wc.jar ProblemOne*.class
hdfs dfs -rm -r /project/problems/one
hadoop jar wc.jar ProblemOne /project/states /project/problems/one/final
rm -rf problems/one/*
hdfs dfs -copyToLocal /project/problems/one/* problems/one/.
