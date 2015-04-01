/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.client.beans;

import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class TokenBean extends ResourceBean {
    private static final long serialVersionUID = -5939191095081672450L;

    private String access_token;
	private String token_type;
	private String token_scope;
	private long expires_in;
	private String name;
	private String short_name;
	
	public TokenBean() {}

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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TokenBean)) return false;

        TokenBean tokenBean = (TokenBean) o;

        if (expires_in != tokenBean.expires_in) return false;
        if (access_token != null ? !access_token.equals(tokenBean.access_token) : tokenBean.access_token != null)
            return false;
        if (name != null ? !name.equals(tokenBean.name) : tokenBean.name != null) return false;
        if (short_name != null ? !short_name.equals(tokenBean.short_name) : tokenBean.short_name != null) return false;
        if (token_scope != null ? !token_scope.equals(tokenBean.token_scope) : tokenBean.token_scope != null)
            return false;
        if (token_type != null ? !token_type.equals(tokenBean.token_type) : tokenBean.token_type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = access_token != null ? access_token.hashCode() : 0;
        result = 31 * result + (token_type != null ? token_type.hashCode() : 0);
        result = 31 * result + (token_scope != null ? token_scope.hashCode() : 0);
        result = 31 * result + (int) (expires_in ^ (expires_in >>> 32));
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (short_name != null ? short_name.hashCode() : 0);
        return result;
    }
}
