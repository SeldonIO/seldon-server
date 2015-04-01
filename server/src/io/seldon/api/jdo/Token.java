/*
 * Seldon -- open source prediction engine
 * =======================================
 *
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 * ********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************************************
 */

package io.seldon.api.jdo;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Date;
import io.seldon.api.Constants;

/**
 * @author claudio
 */

public class Token {
	public final static String DEFAULT_SCOPE = "all";
	public final static String DEFAULT_TYPE = "rummblelabs";
	public final static int TOKEN_MIN_LEN = 15;
	
	private final static SecureRandom random =  new SecureRandom();
	
	//ATTRIBUTES
	String tokenKey; //token value
	Date time; //token creation time
	String type; //token type
	String scope; //token scope
	long expires_in; //token expire time
	boolean active; //show if the token is valid or not
	Consumer consumer; //consumer owner of the token
	
	//CONSTRUCTOR
	public Token(Consumer consumer) {
		tokenKey = randomString(12);
		time = new Date();
		type = DEFAULT_TYPE;
		if(consumer.getScope() != null) {
			scope = consumer.getScope();
		}
		else {
			scope = DEFAULT_SCOPE;
		}
		expires_in = Constants.TOKEN_TIMEOUT;
		active = true;
		this.consumer = consumer;
	}
	
	//GETTERS AND SETTERS
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public String getScope() {
		return scope;
	}
	public void setScope(String scope) {
		this.scope = scope;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public long getExpires_in() {
		return expires_in;
	}
	public void setExpires_in(long expiresIn) {
		expires_in = expiresIn;
	}
	public String getKey() {
		return tokenKey;
	}
	public void setKey(String key) {
		this.tokenKey = key;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public Consumer getConsumer() {
		return consumer;
	}
	public void setConsumer(Consumer consumer) {
		this.consumer = consumer;
	}
	
	//METHODS
	public String randomString(int size) {
		return new BigInteger(130,random).toString(32);
	}
}
