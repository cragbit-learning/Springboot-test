package com.sanjiv.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;

import com.sanjiv.model.Topic;

@Service
public class TopicService {
	
	
	private List<Topic> topics = new ArrayList<Topic>(Arrays.asList(
				new Topic("spring","spring in action","nice topic"),
				new Topic("java","java 8 features","java is good"),
				new Topic("kafka","kafka in 24 hour","nice book"),
				new Topic("hadoop","hadoop learning","good topic")
			));
	
	
	public List<Topic> getAllTopics(){
		return topics;
	}

	public Topic getTopic(String id) {
		return topics.stream().filter(t -> t.getId().equals(id)).findAny().orElse(null);
	}
	
	public Topic createTopics(Topic topic){
		topics.add(topic);
		return topic;
	}
	
	

}
