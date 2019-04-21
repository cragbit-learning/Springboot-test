package com.sanjiv.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.web.client.RestTemplate;

import com.sanjiv.repository.UserRepository;

@SpringBootApplication
@ComponentScan(basePackages = { "com.sanjiv" })
@EntityScan("com.sanjiv.model")
@EnableJpaRepositories("com.sanjiv.repository")
public class SpringbootTestApplication implements CommandLineRunner {

	// private static final Logger logger =
	// LoggerFactory.getLogger(SpringbootTestApplication.class);

	private static final Logger logger =  LoggerFactory.getLogger(SpringbootTestApplication.class);

	@Autowired
	UserRepository userRepository;

	public static void main(String[] args) {
		SpringApplication.run(SpringbootTestApplication.class, args);
		/*
		 * Timer timer = new Timer(); try { timer.log(); } catch (InterruptedException
		 * e) { e.printStackTrace(); }
		 */
	}
	
	@Bean
	public RestTemplate restTemplate() {
	    return new RestTemplate();
	}

	@Override
	public void run(String... args) throws Exception {

//		logger.info("############Running inside run method : ###############");
//
//		addUser();
//		List<User> users = userRepository.findByNameAndEmail("sanjiv kumar", "sanjiv.kumar@happiestminds.com");
//		for (User user : users) {
//			logger.info("User details : " + user);
//		}
//		List<String> emails = new ArrayList<>();
//		emails.add("sanjiv.kumar@happiestminds.com");
//		emails.add("sanjiv.programmer@gmail.com");
//
//		List<User> usersResult = userRepository.findByNameAndEmailIn("sanjiv kumar", emails);
//		for (User user : usersResult) {
//			logger.info("User details : " + user);
//		}
	}

	private void addUser() {

//		User user1 = new User();
//		user1.setEmail("sanjiv.kumar@happiestminds.com");
//		user1.setName("sanjiv kumar");
//
//		userRepository.save(user1);
//
//		User user2 = new User();
//		user2.setEmail("sanjiku5@in.ibm.com");
//		user2.setName("sanjiv kumar");
//
//		userRepository.save(user2);
//
//		User user3 = new User();
//		user3.setEmail("sanjiv.programmer@gmail.com");
//		user3.setName("sanjiv kumar");
//
//		userRepository.save(user3);

	}
}

 class Timer {
	 
	private static final Logger logger = LoggerFactory.getLogger(Timer.class);
    public void log() throws InterruptedException {
		
		  while(true) {
			  logger.info("Inside scheduleTask - Sending logs to Kafka at " +
		  DateTimeFormatter.ofPattern("HH:mm:ss").format(LocalDateTime.now()));
		  Thread.sleep(3000);
		  }
		 
    }
}
