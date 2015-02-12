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

package io.seldon.api.resource;

import org.springframework.stereotype.Component;
import io.seldon.api.Constants;
import io.seldon.api.jdo.Token;

/**
 * @author claudio
 */

@Component
public class TokenBean extends ResourceBean {

	private String access_token;
	private String token_type;
	private String token_scope;
	private long expires_in;
	private String name;
	private String short_name;
	
	public TokenBean() {
		name = Constants.TOKEN_RESOURCE_NAME;;
	}
	
	public TokenBean(Token t) {
		this.access_token = t.getKey();
		this.expires_in = t.getExpires_in();
		this.token_scope = t.getScope();
		this.name = Constants.TOKEN_RESOURCE_NAME;
		this.token_type = t.getType();
		this.short_name = t.getConsumer().getShort_name();
	}
	
	public String getAccess_token() {
		return access_token;
	}
	public void setAccess_token(String accessToken) {
		access_token = accessToken;
	}
	public String getToken_type() {
		return token_type;
	}
	public void setToken_type(String tokenType) {
		token_type = tokenType;
	}
	public String getToken_scope() {
		return token_scope;
	}
	public void setToken_scope(String tokenScope) {
		token_scope = tokenScope;
	}
	public long getExpires_in() {
		return expires_in;
	}
	public void setExpires_in(long expiresIn) {
		expires_in = expiresIn;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public String shortName() {
		return short_name;
	}

	@Override
	public String toKey() {
		return access_token;
	}
	
}
