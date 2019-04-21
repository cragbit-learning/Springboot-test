package com.sanjiv.service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

	private static final String BOOTSTRAP_SERVER = "localhost:9092";
	private static final String CONSUMER_GROUP = "ashok-consumer-group";
	private static final String AUTO_OFFSETS_RESET = "earliest";
	private String topic = "first-topic";
	private Properties producerProperties = new Properties();
	private Properties consumerProperties = new Properties();
	private KafkaProducer<String, String> producer;
	private KafkaConsumer<String, String> consumer;
	

	private static final Logger log =  LoggerFactory.getLogger(KafkaService.class);

	@PostConstruct
	public void customProperties() {
		try {
			initializeProducerProperties();
			initializeConsumerProperties();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void initializeProducerProperties() {
		try {
			producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
			producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producer = new KafkaProducer<>(producerProperties);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private void initializeConsumerProperties() {
		try {
			consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
			consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
			consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSETS_RESET);
			consumer = new KafkaConsumer<>(consumerProperties);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void produceMessage(String message) {

		if (message == null) {
			return;
		}
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, message);
		try {
			producer.send(producerRecord);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void produceMessageWithCallback(String message) {

		if (message == null) {
			return;
		}
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, message);
		try {
			producer.send(producerRecord, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						log.info("Received message :" + "\n Topic : " + metadata.topic() + "\n Partition :  "
								+ metadata.partition() + "\n Offset : " + metadata.offset() + "\n Timestamp : "
								+ metadata.timestamp());
					} else {
						log.error("Error while producing message" + exception);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void produceMessageWithKey(String key, String message) {

		if (message == null) {
			return;
		}
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, message);
		try {
			producer.send(producerRecord, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						log.info("Received message :" + "\n Topic : " + metadata.topic() + "\n Partition :  "
								+ metadata.partition() + "\n Offset : " + metadata.offset() + "\n Timestamp : "
								+ metadata.timestamp());
					} else {
						log.error("Error while producing message" + exception);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void consumeMessage() {
		try {
			consumer.subscribe(Arrays.asList(topic));  
			while(true) {
				ConsumerRecords<String, String> consumerRecords =  consumer.poll(Duration.ofSeconds(5)); 
				for(ConsumerRecord<String, String> record : consumerRecords) {
					log.info("Consumed message : \n" + record.toString() + 
					"\n Topic : " +record.topic() +
					"\n Offset : " +record.offset() +
					"\n Key : " +record.key() +
					"\n Partition : " +record.partition()	
					);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void consumeSpecificMessage() {
		try {
			//assign
			Long offsetToReadFrom  = 10L;
			int numberOfMessageToRead = 5;
			int counter = 0;
			boolean keepReading = true;
			
			TopicPartition topicPartition = new TopicPartition(topic,0);
			consumer.assign(Arrays.asList(topicPartition));  
			//seek
			consumer.seek(topicPartition, offsetToReadFrom); 
		
			while(keepReading) {
				ConsumerRecords<String, String> consumerRecords =  consumer.poll(Duration.ofSeconds(5)); 
				for(ConsumerRecord<String, String> record : consumerRecords) {
					counter++;
					log.info("Consumed message : \n" + record.toString() + 
					"\n Topic : " +record.topic() +
					"\n Offset : " +record.offset() +
					"\n Key : " +record.key() +
					"\n Partition : " +record.partition()	
					);
					if(counter >= numberOfMessageToRead) {
						keepReading = false;
						break;
					}
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
