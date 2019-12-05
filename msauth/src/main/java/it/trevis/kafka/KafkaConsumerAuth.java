package it.trevis.kafka;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerAuth {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	private KafkaConsumer<String, String> consumer;
	
	private List<String> topic;
	
	private KafkaConsumer<String,String> createConsumer() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "auth-service-login";
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(this.topic);
		return consumer;
	}
	
	public String receive() {
		while(true) {
			Map<String,String> userMap = new HashMap<>();
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for(ConsumerRecord<String, String> record: records) {
				logger.info("Key: {} , Value: {} , Partition: {} , Offset: {}", 
						record.key(), record.value(), record.partition(), record.offset());
				userMap.put(record.key(), record.value());
			}
			return "";
		}
	}
	
	public KafkaConsumerAuth(List<String> topicList) {
		this.topic = topicList;
		this.consumer = createConsumer();
	}

	public static void main(String[] args) {
		
	}

}
