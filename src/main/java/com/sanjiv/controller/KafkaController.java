package com.sanjiv.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sanjiv.service.KafkaProducerService;
import com.sanjiv.service.KafkaService;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {
	
	@Autowired
	private KafkaProducerService producer;
	
	@Autowired
	private KafkaService kafkaService;

	@PostMapping(value = "/publish")
	public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
		this.producer.sendMessage(message);
	}
	
	@PostMapping(value = "/msg")
	public void sendMessage(@RequestParam("message") String message) {
		this.kafkaService.produceMessage(message);
	}
	
	@PostMapping(value = "/callbak-msg")
	public void sendMessageWithCallback(@RequestParam("message") String message) {
		this.kafkaService.produceMessageWithCallback(message);
	}
	
	@PostMapping(value = "/key-msg")
	public void sendMessageWithKey(@RequestParam("message") String message,@RequestParam("key") String key) {
		this.kafkaService.produceMessageWithKey(key, message);
	}
	
	@GetMapping(value = "/consume")
	public void consumeMessage() {
		this.kafkaService.consumeMessage();
	}
	
	@GetMapping(value = "/consume-specific")
	public void consumeMessageSpecific() {
		this.kafkaService.consumeSpecificMessage();
	}
}