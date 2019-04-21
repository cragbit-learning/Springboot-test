package com.sanjiv.advice;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.sanjiv.dto.MessageDTO;

@RestControllerAdvice
public class ProductAdvice {
	
	@ExceptionHandler
	@ResponseBody
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public MessageDTO processNullPointerException(NullPointerException exception) {
		
		MessageDTO messageDTO = new MessageDTO();
		messageDTO.setMessage("Error found in request, try again later");
		messageDTO.setType("Error");
		return messageDTO;
	}
	
	@ExceptionHandler
	@ResponseBody
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	public MessageDTO procesIllegalArgumentException(IllegalArgumentException exception) {
		
		MessageDTO messageDTO = new MessageDTO();
		messageDTO.setMessage("Argument missing, please check palyload");
		messageDTO.setType("Error");
		return messageDTO;
	}
	
	

}
