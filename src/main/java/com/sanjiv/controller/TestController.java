package com.sanjiv.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.sanjiv.service.ProfileService;

@RestController
@RequestMapping("/test")
public class TestController {
	
	@Autowired
	private  ProfileService profileService;

	@RequestMapping(path = "/hello/{name}", method = RequestMethod.GET)
	public String home(@PathVariable("name") String nameVar) {
		return "Hello " + nameVar;
	}
	
	@RequestMapping(path = "/{profile}", method = RequestMethod.GET)
	public String activeProfile(@PathVariable("profile") String profile) {
		return profileService.acticeProfile(profile);
	}

}
