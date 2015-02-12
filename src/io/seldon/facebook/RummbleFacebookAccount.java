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

package io.seldon.facebook;

public class RummbleFacebookAccount {

	private long id;
	private String oAuthToken;
	private Boolean tokenActive;
	private Long fbId;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getoAuthToken() {
		return oAuthToken;
	}
	public void setoAuthToken(String oAuthToken) {
		this.oAuthToken = oAuthToken;
	}
	public boolean isTokenActive() {
		return tokenActive;
	}
	public void setTokenActive(Boolean tokenActive) {
		this.tokenActive = tokenActive;
	}
	public Long getFbId() {
		return fbId;
	}
	public void setFbId(Long fbId) {
		this.fbId = fbId;
	}

    public Boolean getTokenActive() {
        return tokenActive;
    }
}
