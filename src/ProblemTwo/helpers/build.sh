rm ProblemTwo*.class
hadoop com.sun.tools.javac.Main ProblemTwo.java
jar cf wc.jar ProblemTwo*.class
hdfs dfs -rm -r /project/problems/two
hadoop jar wc.jar ProblemTwo /project/states /project/problems/two/final
rm -rf problems/two/*
hdfs dfs -copyToLocal /project/problems/two/* problems/two/.
