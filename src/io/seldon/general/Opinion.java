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

public class Opinion {

	long opId;
	long userId;
	long itemId;
	Double value;
	Date time;
	
	public Opinion() {};
	
	
	public Opinion(long opId, long userId, long itemId, Double value, Date time) {
		this.opId = opId;
		this.userId = userId;
		this.itemId = itemId;
		this.value = value;
		this.time = time;
	}
	public Opinion(long userId, long itemId, Double value, Date time) {
		this.userId = userId;
		this.itemId = itemId;
		this.value = value;
		this.time = time;
	}


	public long getOpId() {
		return opId;
	}
	public void setOpId(long opId) {
		this.opId = opId;
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
	public Double getValue() {
		return value;
	}
	public void setValue(Double value) {
		this.value = value;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	
	
}
