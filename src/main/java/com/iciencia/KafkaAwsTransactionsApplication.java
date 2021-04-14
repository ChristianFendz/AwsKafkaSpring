package com.iciencia;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.iciencia.model.PersonaTransaction;

@SpringBootApplication
@EnableScheduling
public class KafkaAwsTransactionsApplication {

	private static final Logger log = LoggerFactory.getLogger(KafkaAwsTransactionsApplication.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaProducer;

	@Autowired
	private ObjectMapper mapper;

	@Autowired
	private RestHighLevelClient client;

	/**
	 * Este metodo recibe los mensajes de kafka y los guarda en elastickSearch de
	 * forma sincrona
	 * 
	 * @param messages
	 * @throws JsonMappingException
	 * @throws JsonProcessingException
	 */
	@KafkaListener(topics = "iciencia-transactions", groupId = "iciencia-group")
	public void listen(List<ConsumerRecord<String, String>> messages)
			throws JsonMappingException, JsonProcessingException {
		for (ConsumerRecord<String, String> message : messages) {

			IndexRequest indexRequest = buildIndexReques(
					String.format("%s-%s-%s", message.partition(), message.key(), message.offset()), message.value());

			client.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {

				@Override
				public void onResponse(IndexResponse response) {
					log.debug("Successful request");
				}

				@Override
				public void onFailure(Exception e) {
					log.error("Error storing message {}", e);
				}
			});
		}
	}

	private IndexRequest buildIndexReques(String key, String value) {
		IndexRequest indexRequest = new IndexRequest("iciencia-transaction");
		indexRequest.id(key);
		indexRequest.source(value, XContentType.JSON);

		return indexRequest;
	}

	@Scheduled(fixedDelay = 15000)
	public void sendMessages() throws JsonProcessingException {
		Faker faker = new Faker();

		for (int i = 0; i < 10000; i++) {
			PersonaTransaction persona = new PersonaTransaction();
			persona.setApellido(faker.name().lastName());
			persona.setUsername(faker.name().username());
			persona.setNombre(faker.name().firstName());
			persona.setMonto(
					new BigDecimal(faker.number().randomDouble(4, 400, 5000)).setScale(4, RoundingMode.HALF_DOWN));

			kafkaProducer.send("iciencia-transactions", persona.getUsername(), mapper.writeValueAsString(persona));
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaAwsTransactionsApplication.class, args);
	}

//	@Override
//	public void run(String... args) throws Exception {
//		IndexRequest indexRequest = new IndexRequest("iciencia-transaction");
//		indexRequest.id("54");
//		
//		indexRequest.source("{\n"
//				+ "    \"nombre\": \"Donaldx\",\n"
//				+ "    \"apellido\": \"Daugherty\",\n"
//				+ "    \"username\": \"tresa.cummings\",\n"
//				+ "    \"monto\": 1486.3065\n"
//				+ "}",XContentType.JSON);
//		
//		IndexResponse response =  client.index(indexRequest, RequestOptions.DEFAULT);
//		
//		
//		log.info("REsponse {}", response.getId());
//	}

}
