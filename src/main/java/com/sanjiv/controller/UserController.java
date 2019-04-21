package com.sanjiv.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.google.gson.JsonObject;
import com.sanjiv.model.User;
import com.sanjiv.repository.UserRepository;
import com.sanjiv.service.KafkaProducerService;

@Controller
@RequestMapping(path = "/users")
public class UserController {
	
	@Autowired
	UserRepository userRepository;
	
	@Autowired
	KafkaProducerService kafkaProducer;

	@RequestMapping(path = "/signup", method = RequestMethod.GET)
	public String showSignUpForm(User user) {
		return "add-user";
	}
	
	@RequestMapping(path = "", method = RequestMethod.GET)
	public String showUser(Model model) {
		model.addAttribute("users", userRepository.findAll());
		return "index";
	}

	@RequestMapping(path = "/adduser", method = RequestMethod.POST)
	public String addUser(@Valid User user, BindingResult result, Model model) {
		if (result.hasErrors()) {
			return "add-user";
		}
		User userDetail = userRepository.save(user);
		if(userDetail!=null) {
			JsonObject person = new JsonObject();
			person.addProperty("userId", userDetail.getId());
			person.addProperty("action", "created");
			kafkaProducer.sendMessage(person.toString());
		}
		//model.addAttribute("users", userRepository.findAll(PageRequest.of(0, 2)));
		model.addAttribute("users", userRepository.findAll());
		return "index";
	}

	@RequestMapping(path = "/edit/{id}", method = RequestMethod.GET)
	public String showUpdateForm(@PathVariable("id") long id, Model model) {
		User user = userRepository.findById(id)
				.orElseThrow(() -> new IllegalArgumentException("Invalid user Id:" + id));
		model.addAttribute("user", user);
		return "update-user";
	}

	@RequestMapping(path = "/update/{id}", method = RequestMethod.POST)
	public String updateUser(@PathVariable("id") long id, @Valid User user, BindingResult result, Model model) {
		if (result.hasErrors()) {
			user.setId(id);
			return "update-user";
		}

		User userDetail = userRepository.save(user);
		
		if(userDetail!=null) {
			JsonObject person = new JsonObject();
			person.addProperty("userId", userDetail.getId());
			person.addProperty("action", "updated");
			kafkaProducer.sendMessage(person.toString());
		}
		
		model.addAttribute("users", userRepository.findAll());
		return "index";
	}

	@RequestMapping(path = "/delete/{id}", method = RequestMethod.GET)
	public String deleteUser(@PathVariable("id") long id, Model model) {
		User user = userRepository.findById(id)
				.orElseThrow(() -> new IllegalArgumentException("Invalid user Id:" + id));
		
		userRepository.delete(user);
		
		if(id!=0) {
			JsonObject person = new JsonObject();
			person.addProperty("userId", id);
			person.addProperty("action", "deleted");
			kafkaProducer.sendMessage(person.toString());
		}
		
		model.addAttribute("users", userRepository.findAll());
		return "index";
	}
}