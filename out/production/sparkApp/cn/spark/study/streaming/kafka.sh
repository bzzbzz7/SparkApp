启动kafka
bin/kafka-server-start.sh config/server.properties &
创建topic
bin/kafka-topics.sh --zookeeper hadoop.zhengzhou.com:2181 --topic WordCount --replication-factor 1 --partitions 1 --create
查看topic
bin/kafka-topics.sh --zookeeper hadoop.zhengzhou.com:2181 --list

创建生产者
bin/kafka-console-producer.sh --broker-list hadoop.zhengzhou.com:9092 --topic WordCount

