package com.sanjiv.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("german")
public class GermanProfileService implements ProfileService {

	@Override
	public String acticeProfile(String profile) {
		return "Halo "+ profile;
	}

}
