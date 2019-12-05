package it.trevis.msauth;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JacksonInject.Value;
import com.google.inject.Key;

import it.trevis.kafka.KafkaConsumerAuth;
import it.trevis.kafka.KafkaProducerAuth;

@Path("/login")
public class AuthService {
	
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	@Inject
	private KafkaProducerAuth kPub; //Publisher
	
	@Inject
	private KafkaConsumerAuth kSub; //Subscriber
	
	@Context
	private HttpServletRequest httpRequest;
	
	private static String TOPIC_IN = "userDataReq";
	private static String TOPIC_OUT = "userDataRes";
	
	Base64.Encoder enc = Base64.getEncoder();
	
	@POST
	@Path("/registration")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postRegistration() {
		String id = UUID.randomUUID().toString();
		logger.info("Post auth call start con id: {}", id);
		
		// kSub
		// logger.info("Inizio polling..");
		// String user = kSub.receive();
		Future<RecordMetadata> futureCall = null;
		RecordMetadata result = null;
		try {
			futureCall = kPub.sendReq(TOPIC_IN,enc.encodeToString(IOUtils.toByteArray(httpRequest.getInputStream())));
			result = futureCall.get();
		} catch (IOException | InterruptedException | ExecutionException e) {
			logger.error("Errore nella conversione da inputStream a byteArray..", e);
		}
		logger.info("Post auth call: dati inviati a kafka..");
		return null;
	}
	
	@POST
	@Path("/login")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response postLogin() {
		
		Properties properties = new Properties();
		
		String id = "id_1";
				// UUID.randomUUID().toString();
		logger.info("Post auth call start con id: {}", id);
		// Topology topology = new Topology();
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String,String> loginStream = builder.stream(TOPIC_IN);
		KStream<String,String> loginStreamFiltered = loginStream
				.filter((key, value) -> (key.equals(id)));
		// loginStreamFiltered.to(TOPIC_OUT);
		// loginStreamFiltered.;
//		KTable<String,String> loginTable = builder.table(TOPIC_IN);
//		KTable<String,String> loginTableFiltered = loginTable
//				.filter( (tkey, tvalue) -> (tkey.equals(id)) );
		loginStreamFiltered.to(TOPIC_OUT);
		// KafkaStreams stream = new KafkaStreams(builder, properties);
		// stream.start();
		logger.info("END of kafka stream..");
		return null;
	}
	
	public static void main(String[] args) {
		logger.info("Main start..");
	}

}
