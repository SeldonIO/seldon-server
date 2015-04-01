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

package io.seldon.general;

import java.util.Date;
import java.util.List;

import io.seldon.api.Constants;

public class Action {

	Long actionId;
	long userId;
	long itemId;
	Integer type;
	Integer times = Constants.DEFAULT_TIMES;
	Date date;
	Double value;
	String comment;
	String clientUserId;
	String clientItemId;
	List<String> tags;
	
	public Action() {
		date = new Date();
		value = null;
		comment = null;
	}
	
	public String toString()
	{
		StringBuffer b = new StringBuffer();
		b.append("Action(").append("userId:").append(userId).append(" itemId:").append(itemId).append(" clientUserId:").append(clientUserId).append(" clientItemId:").append(clientItemId);
		return b.toString();
	}
	
	public Action(Long actionId, long userId, long itemId, Integer type, Integer times, Date date, Double value, String clientUserId,String clientItemId) {
		this.actionId = actionId;
		this.userId = userId;
		this.itemId = itemId;
		this.type = type;
		this.times = times;
		this.date = date;
		this.value = value;
		this.comment = null;
		this.clientUserId = clientUserId;
		this.clientItemId = clientItemId;
	}
	
	
	public Action(Long userId, Long itemId, int type,
			Date date, Double value, String comment, List<String> tags) {
		this.userId = userId;
		this.itemId = itemId;
		this.type = type;
		this.date = date;
		this.value = value;
		this.comment = comment;
		this.tags = tags;
	}

	public Action(Long userId, Long itemId, int type,
			Date date, Double value) {
		this.userId = userId;
		this.itemId = itemId;
		this.type = type;
		this.date = date;
		this.value = value;
		this.comment = null;
		this.tags = null;
	}

	public Long getActionId() {
		return actionId;
	}
	public void setActionId(Long actionId) {
		this.actionId = actionId;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public long getItemId() {
		return itemId;
	}
	public void setItemId(long itemId) {
		this.itemId = itemId;
	}
	public Integer getType() {
		return type;
	}
	public void setType(Integer type) {
		this.type = type;
	}
	public Integer getTimes() {
		return times;
	}
	public void setTimes(Integer times) {
		this.times = times;
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

	public String getComment() {
		return comment;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}
	

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getClientUserId() {
		return clientUserId;
	}

	public void setClientUserId(String clientUserId) {
		this.clientUserId = clientUserId;
	}

	public String getClientItemId() {
		return clientItemId;
	}

	public void setClientItemId(String clientItemId) {
		this.clientItemId = clientItemId;
	}

	
}
