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


import java.util.Date;

import io.seldon.api.resource.service.UserService;
import io.seldon.general.ExplicitLink;
import org.springframework.stereotype.Component;

import io.seldon.api.Constants;

/**
 * @author claudio
 */

@Component
public class UserTrustNodeBean extends ResourceBean {
	
	//user owner of the graph
	String center;
	String user;
	Double trust = null;
	Long pos;
	Integer dimension;
	
	public UserTrustNodeBean() {
	}
	
	public UserTrustNodeBean(String center,String user, double trust,long pos, int dimension) {
		this.user = user;
		this.trust = trust;
		this.pos = pos;
		this.dimension = dimension;
	}

	public Double getTrust() {
		return trust;
	}

	public void setTrust(Double trust) {
		this.trust = trust;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Long getPos() {
		return pos;
	}

	public void setPos(Long pos) {
		this.pos = pos;
	}
	
	public String toKey() {
		return user;
	}

	public Integer getDimension() {
		return dimension;
	}

	public void setDimension(Integer dimension) {
		this.dimension = dimension;
	}

	public int compareTo(UserTrustNodeBean o) {
		if(this.trust == o.trust)
			return this.user.compareTo(o.user);
		else if(this.trust > o.trust)
			return -1;
		else 
			return 1;
	}
	
	
	public ExplicitLink createExplicitLink(ConsumerBean c) {
		ExplicitLink e = new ExplicitLink();
		e.setU1(UserService.getInternalUserId(c, this.center));
		e.setU2(UserService.getInternalUserId(c, this.user));
		e.setDate(new Date());
		e.setValue(this.trust);
		if(this.dimension == null) {
			e.setType(Constants.DEFAULT_DIMENSION);
		}
		else {
			e.setType(this.dimension);
		}
		return e;
	}

	public void setCenter(String userId) {
		this.center = userId;
	}
}
