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
import java.util.List;

import io.seldon.api.APIException;
import io.seldon.api.resource.service.ItemService;
import org.springframework.stereotype.Component;

import io.seldon.api.resource.service.UserService;
import io.seldon.general.Action;
import io.seldon.general.ExtAction;

@Component
public class ActionBean extends ResourceBean {

	Long actionId;
	String user;
	String item;
	int type;
	Date date;
	Double value;
	Integer times;
	String comment;
	List<String> tags;
	String recTag;
	String referrer;
	
	ActionBean() {}
	
	public ActionBean(Long actionId, String user, String item, int type, Date date, Double value, int times) {
		this.actionId = actionId;
		this.user = user;
		this.item = item;
		this.type = type;
		this.date = date;
		this.value = value;
		this.times = times;
	}
	
	public ActionBean(Action a,ConsumerBean c, boolean full) throws APIException {
		user = UserService.getClientUserId(c, a.getUserId());
		item = ItemService.getClientItemId(c, a.getItemId());
		actionId = a.getActionId();
		type = a.getType();
		date = a.getDate();
		value = a.getValue();
		times = a.getTimes();
		comment = a.getComment();
		tags = a.getTags();
	}
	
	public ActionBean(ExtAction a,ConsumerBean c, boolean full) throws APIException {
		user = UserService.getClientUserId(c, a.getUserId());
		item = ItemService.getClientItemId(c, a.getItemId());
		actionId = a.getActionId();
		type = a.getType();
		date = a.getDate();
		value = a.getValue();
		times = a.getTimes();
		comment = a.getComment();
		tags = a.getTags();
	}
	
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Integer getTimes() {
		return times;
	}

	public void setTimes(Integer times) {
		this.times = times;
	}

	public Long getActionId() {
		return actionId;
	}

	public void setActionId(Long actionId) {
		this.actionId = actionId;
	}
		
	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getComment() { return this.comment; }
	
	public void setComment(String c) { this.comment=c; }
	
	
	
	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}
	
	

	public String getRecTag() {
		return recTag;
	}

	public void setRecTag(String recTag) {
		this.recTag = recTag;
	}

	public Action createAction(ConsumerBean c) {
		Long itemId = ItemService.getInternalItemId(c, this.getItem());
		Long userId = UserService.getInternalUserId(c, this.getUser());
		return createAction(c,userId,itemId);
	}
	
	public Action createAction(ConsumerBean c,Long userId,Long itemId)
	{
		Action a = new Action();
		a.setItemId(itemId);
		a.setUserId(userId);
		a.setClientUserId(this.user);
		a.setClientItemId(this.item);
		if(this.getDate() != null) { a.setDate(this.getDate());}
		a.setValue(this.getValue());
		a.setComment(this.getComment());
		a.setType(this.getType());
		if(this.getTimes() != null) {
			a.setTimes(this.getTimes());
		}
		a.setTags(this.getTags());
		return a;
	}

	@Override
	public String toKey() {
		return actionId+"";
	}
	
	public String toString()
	{
		return "ActionId:"+actionId+" user:"+user+" item:"+item;
	}
	
	@Override
	public String toLog() {
		return user+";"+item+";"+type;
	}

	public ExtAction createExtAction(ConsumerBean c, Long userId, Long itemId) {
		ExtAction a = new ExtAction();
		a.setItemId(itemId);
		a.setUserId(userId);
		a.setClientUserId(this.user);
		a.setClientItemId(this.item);
		if(this.getDate() != null) { a.setDate(this.getDate());}
		a.setValue(this.getValue());
		a.setComment(this.getComment());
		a.setType(this.getType());
		if(this.getTimes() != null) {
			a.setTimes(this.getTimes());
		}
		a.setTags(this.getTags());
		return a;
	}
}

