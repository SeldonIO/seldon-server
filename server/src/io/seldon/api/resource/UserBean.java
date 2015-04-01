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
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.service.ItemService;
import io.seldon.general.UserDimension;
import org.springframework.stereotype.Component;

import io.seldon.api.Constants;
import io.seldon.general.User;

/**
 * @author claudio
 */

@Component
public class UserBean extends ResourceBean {

	private String id;
	private String username;
	private Date first_action;
	private Date last_action;
	int type;
	int num_actions;
	boolean active;
	ArrayList<DimensionBean> dimensions;
	Map<Integer,Integer> attributes;
	Map<String,String> attributesName;
	
	public UserBean() {
	}
	
	public UserBean(String id) {
		this.id = id;
	}

	public UserBean(String id, String username) {
		this.id = id;
		this.username = username;
	}

	public UserBean(User u,boolean full,ConsumerBean c) throws APIException {
		id = u.getClientUserId();
		username = u.getUsername();
		first_action = u.getFirstOp();
		last_action = u.getLastOp();
		type = u.getType();
		num_actions = u.getNumOp();
		active = u.isActive();
		if(full) {
			//Attributes
			attributes = Util.getUserAttributePeer(c).getUserAttributes(u.getUserId());
			attributesName = Util.getUserAttributePeer(c).getUserAttributesName(u.getUserId());	
			//Dimensions
			dimensions = new ArrayList<>();
			Collection<UserDimension> dims = Util.getUserPeer(c).getUserDimensions(u.getUserId());
			for(UserDimension ud : dims) {
				DimensionBean d = ItemService.getDimension(c, ud.getDimId());
				d.setAmount(ud.getRelevance());
				dimensions.add(d);
			}
		}
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Date getFirst_action() {
		return first_action;
	}

	public void setFirst_action(Date firstAction) {
		first_action = firstAction;
	}

	public Date getLast_action() {
		return last_action;
	}

	public void setLast_action(Date lastAction) {
		last_action = lastAction;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getNum_actions() {
		return num_actions;
	}

	public void setNum_actions(int numActions) {
		num_actions = numActions;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public ArrayList<DimensionBean> getDimensions() {
		return dimensions;
	}

	public void setDimensions(ArrayList<DimensionBean> dimensions) {
		this.dimensions = dimensions;
	}

	@Override
	public String toKey() {
		return id;
	}

	
	public Map<Integer, Integer> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<Integer, Integer> attributes) {
		this.attributes = attributes;
	}

	public Map<String, String> getAttributesName() {
		return attributesName;
	}

	public void setAttributesName(Map<String, String> attributesName) {
		this.attributesName = attributesName;
	}

	public User createUser(ConsumerBean c) {
		User u = new User();
		u.setClientUserId(this.id);
		u.setActive(this.active);
		u.setFirstOp(new Date());
		u.setLastOp(new Date());
		u.setType(Constants.DEFAULT_USER_TYPE);
		u.setNumOp(0);
		u.setUsername(this.username);
		return u;
	}

	@Override
	public String toLog() {
		return id+";"+type;
	}
	
}
