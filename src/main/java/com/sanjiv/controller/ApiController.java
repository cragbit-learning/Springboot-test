package com.sanjiv.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.sanjiv.model.User;
import com.sanjiv.repository.UserRepository;

@RestController
@RequestMapping("/api")
public class ApiController {

	@Autowired
	UserRepository userRepository;
	
	
	@RequestMapping(path = "users/{id}", method = RequestMethod.GET)
	public User getUser(@PathVariable("id") long id) {
		return userRepository.findById(id).orElse(null); 
	}
	
	

}