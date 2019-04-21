package com.sanjiv.model;

public class UserTemplate {
	
	private Integer userId;
	private String action;
	
	public UserTemplate(Integer userId, String action) {
		super();
		this.userId = userId;
		this.action = action;
	}
	public Integer getUserId() {
		return userId;
	}
	public void setUserId(Integer userId) {
		this.userId = userId;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
}
