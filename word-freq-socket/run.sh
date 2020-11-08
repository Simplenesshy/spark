/Users/huying/software/spark-2.4.6-bin-hadoop2.7/bin/spark-submit \
	--class sparkstreaming.socket.main.Socket \
	--num-executors 4 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--executor-cores 1 \
	--conf spark.default.parallelism=1000 \
	./target/word-freq-socket-1.0-SNAPSHOT-jar-with-dependencies.jar
