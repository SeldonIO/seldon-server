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

import java.util.Date;

/**
 * @author claudio
 *
 */
public class Consumer {
	
	//ATTRIBUTES
	String consumerKey; //consumer key
	String consumerSecret; //consumer secret
	String name; //consumer name
	String short_name; //consumer short name (used for tables)
	Date time; //start date
	String url; //consumer url, if set only reques from that domain will be accepted
	boolean active; //boolean to enable/disable an account;
	boolean secure; //if to the consumer is required a secure connection
	String scope; //default scope for issued tokens
	
	//GETTERS AND SETTERS
	public String getKey() {
		return consumerKey;
	}
	public void setKey(String key) {
		this.consumerKey = key;
	}
	public String getSecret() {
		return consumerSecret;
	}
	public void setSecret(String secret) {
		this.consumerSecret = secret;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getShort_name() {
		return short_name;
	}
	public void setShort_name(String shortName) {
		short_name = shortName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public boolean isActive() {
		return active;
	}
	public void setActive(boolean active) {
		this.active = active;
	}
	public boolean isSecure() {
		return secure;
	}
	public void setSecure(boolean secure) {
		this.secure = secure;
	}
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
	
	
}
