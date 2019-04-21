package com.sanjiv.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.sanjiv.model.Topic;
import com.sanjiv.service.KafkaProducerService;
import com.sanjiv.service.TopicService;

@RestController
@RequestMapping("/topics")
public class TopicController {

	@Autowired
	TopicService topicService;
	
	@Autowired
	KafkaProducerService kafkaProducer;

	
	@RequestMapping(method=RequestMethod.GET)
	public List<Topic> getAllTopics() {
		return topicService.getAllTopics();
	}
	
	@RequestMapping(path = "/{id}",method=RequestMethod.GET)
	public Topic getTopic(@PathVariable String id) {
		return topicService.getTopic(id);
	}
	
	@RequestMapping(method=RequestMethod.POST)
	public Topic createTopics(@RequestBody Topic topic) {
		return topicService.createTopics(topic);
	}

}
