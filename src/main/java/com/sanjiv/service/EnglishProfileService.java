package com.sanjiv.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("english")
public class EnglishProfileService implements ProfileService {

	@Override
	public String acticeProfile(String profile) {
		return "Hello " +profile;
	}

}
