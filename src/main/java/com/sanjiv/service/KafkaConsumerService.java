package com.sanjiv.service;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.google.gson.Gson;
import com.sanjiv.model.UserTemplate;

@Service
public class KafkaConsumerService {

	private final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
	
	@Autowired
	RestTemplate restTemplate = new RestTemplate();

	@KafkaListener(topics = "user-records", groupId = "myGroup")
	public void consume(String message) throws IOException {
		logger.info(String.format("#### -> Consumed message -> %s", message));
		try {
			Gson gson = new Gson();
			UserTemplate userTemplate = gson.fromJson(message,UserTemplate.class); 
			logger.info("userId :  " + userTemplate.getUserId() + " action : " + userTemplate.getAction()); 
			
			String fooResourceUrl = "http://localhost:8080/api/users/" + userTemplate.getUserId(); 
			ResponseEntity<String> response = restTemplate.getForEntity(fooResourceUrl , String.class);
			logger.info(response.toString()); 
		}catch (Exception e) {
			logger.info("got error while parsing" + e);
		}
		
		
	}
}
