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

import io.seldon.api.resource.UserBean;

import java.util.Date;

public class User {

	private long userId;
	private String username;
	private Date firstOp;
	private Date lastOp;
	private int type;
	private int numOp;
	private boolean active;
	private String clientUserId;
	private double avgRating;
	private double stdDevRating;
	
	public User() {}
	
	public User(long userId, String username, Date firstOp, Date lastOp, int type, int numOp, boolean active, String clientUserId) {
		this.userId = userId;
		this.username = username;
		this.firstOp = firstOp;
		this.lastOp = lastOp;
		this.type = type;
		this.numOp = numOp;
		this.active = active;
		this.clientUserId = clientUserId;
	}

	
	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Date getFirstOp() {
		return firstOp;
	}

	public void setFirstOp(Date firstOp) {
		this.firstOp = firstOp;
	}

	public Date getLastOp() {
		return lastOp;
	}

	public void setLastOp(Date lastOp) {
		this.lastOp = lastOp;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getNumOp() {
		return numOp;
	}

	public void setNumOp(int numOp) {
		this.numOp = numOp;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public String getClientUserId() {
		return clientUserId;
	}

	public void setClientUserId(String clientUserId) {
		this.clientUserId = clientUserId;
	}

	public double getAvgRating() {
		return avgRating;
	}

	public void setAvgRating(double avgRating) {
		this.avgRating = avgRating;
	}

	public double getStdDevRating() {
		return stdDevRating;
	}

	public void setStdDevRating(double stdDevRating) {
		this.stdDevRating = stdDevRating;
	}

    public static User fromUserBean(UserBean bean, long userId){
        return new User(userId, bean.getUsername(), bean.getFirst_action(), bean.getLast_action(),
                        bean.getType(), bean.getNum_actions(), bean.isActive(), bean.getId());
    }
	
}