package cn.itcast.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


public class ProducerDemo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zk.connect", "node1:2181,node2:2181,node3:2181");
		props.put("metadata.broker.list", "node1:9092,node1:9092,node1:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		for (int i = 1001; i <= 1100; i++)
			producer.send(new KeyedMessage<String, String>("test", "it " + i));
	}
}