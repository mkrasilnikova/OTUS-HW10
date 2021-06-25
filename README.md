# OTUS-HW10


Запуск стриминговой части приложения

$SPARK_HOME/bin/spark-submit --class consumer.ConsumerStreaming  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 homework10-assembly-0.1.jar

Запуск батча

$SPARK_HOME/bin/spark-submit --class consumer.ConsumerBatch   homework10-assembly-0.1.jar 2019-08-01

Запуск consumer'а

$KAFKA_HOME/bin/kafka-console-consumer.sh --topic prediction --bootstrap-server localhost:9092

Запуск producer'а

awk  'NR > 1 {print}' < ~/otus/homework10/data/restaurant-orders.csv | $KAFKA_HOME/bin/kafka-console-producer.sh --topic input --bootstrap-server localhost:9092