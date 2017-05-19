hadoop jar HMR-1.0-SNAPSHOT.jar HadoopMRed '/home/ariel/hadoop/input/source.txt' '/home/ariel/hadoop/output/result'

hadoop fs -cat '/home/ariel/hadoop/output/result/part-r-00000'
