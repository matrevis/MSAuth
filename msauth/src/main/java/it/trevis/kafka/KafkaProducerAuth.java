package it.trevis.kafka;

import java.lang.invoke.MethodHandles;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerAuth {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	
	private KafkaProducer<String,String> producer;
	
	private static KafkaProducer<String, String> createKafkaProducer(){
		String bootstrapServers = "127.0.0.1:9092";
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		// for idempotence producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		// for high throughput producer
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
		return producer;
	}
	
	public void run(String msg) {
		KafkaProducer<String,String> producer = createKafkaProducer();
		
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Closing producetr..");
			// send all msg in memory to kafka before shutting down
			producer.close();
			logger.info("Shutting down done!");
		}));
		
		producer.send(new ProducerRecord<String, String>("twitter_tweets", msg), new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if(e != null) {
					logger.error("Errore", e);
				}
			}
		});
	}
	
	public Future<RecordMetadata> sendReq(String topic,String msg) {
		return this.producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if(e != null) {
					logger.error("Errore nella Callback di sendReq..", e);
				}
			}
		});
	}

	public KafkaProducerAuth() {
		this.producer = createKafkaProducer();
	}

	public static void main(String[] args) {
		logger.info("Start main..");
		KafkaProducerAuth kPub = new KafkaProducerAuth();
		String jsonString = "";
		kPub.run(jsonString);
		logger.info("Stop main..");
	}

}
