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


import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import io.seldon.api.APIException;
import io.seldon.api.resource.service.ItemService;
import org.springframework.stereotype.Component;
import io.seldon.api.resource.service.UserService;
import io.seldon.general.Opinion;

/**
 * @author claudio
 */

@Component
public class OpinionBean extends ResourceBean {

	String item;
	String user;
	Double value;
	boolean prediction;
	Date time;
	List<Long> srcUsers;
	
	public OpinionBean() {};

	public OpinionBean(String u, String i) {
		user = u;
		item = i;
		value = null;
		prediction = true;
		//update with the last update time
		time = new Date();
		srcUsers = null;
	}
	
	public OpinionBean(String u, String i, double value, boolean prediction, Date time) {
		user = u;
		item = i;
		this.value = value;
		this.prediction = prediction;
		this.time = time;
		srcUsers = new ArrayList<>();
	}

	public OpinionBean(Opinion o,ConsumerBean c,boolean full) throws APIException {
		item = ItemService.getClientItemId(c, o.getItemId());
		user = UserService.getClientUserId(c,o.getUserId());
		value = o.getValue();
		prediction = false;
		time  = o.getTime();
		if(full) {
			srcUsers = new ArrayList<>();
			//TODO
		}
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public boolean getPrediction() {
		return prediction;
	}

	public void setPrediction(boolean prediction) {
		this.prediction = prediction;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}


	public List<Long> getSrcUsers() {
		return srcUsers;
	}


	public void setSrcUsers(List<Long> srcUsers) {
		this.srcUsers = srcUsers;
	}

	public void addSrcUser(Long u) {
		srcUsers.add(u);
	}

	public void initSrcUsers() {
		srcUsers = new ArrayList<>();
		
	}

	@Override
	public String toKey() {
		return item+";"+user;
	}
	
}