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

public class Item {

	private long itemId;
	private String name;
	private Date firstOp;
	private Date lastOp;
	private boolean popular;
	private int type;
	private String clientItemId;
	private double avgRating;
	private double stdDevRating;
	
	public Item() {}
	
	

	public Item(long itemId, String name, Date firstOp, Date lastOp, boolean popular, int type, String clientItemId) {
		this.itemId = itemId;
		this.name = name;
		this.firstOp = firstOp;
		this.lastOp = lastOp;
		this.popular = popular;
		this.type = type;
		this.clientItemId = clientItemId;
	}



	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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

	public boolean isPopular() {
		return popular;
	}

	public void setPopular(boolean popular) {
		this.popular = popular;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getClientItemId() {
		return clientItemId;
	}

	public void setClientItemId(String clientItemId) {
		this.clientItemId = clientItemId;
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



    @Override
    public String toString() {
        return "Item [itemId=" + itemId + ", name=" + name + ", firstOp="
                + firstOp + ", lastOp=" + lastOp + ", popular=" + popular
                + ", type=" + type + ", clientItemId=" + clientItemId
                + ", avgRating=" + avgRating + ", stdDevRating=" + stdDevRating
                + "]";
    }
	
	

}