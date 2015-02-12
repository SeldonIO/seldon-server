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

public class UserDimension {
	private int dimId;
	private long userId;
	private double relevance;
	
	public UserDimension() {}
	
	public UserDimension(int dimId, long userId, double relevance) {
		super();
		this.dimId = dimId;
		this.userId = userId;
		this.relevance = relevance;
	}

	public int getDimId() {
		return dimId;
	}

	public void setDimId(int dimId) {
		this.dimId = dimId;
	}

	public long getUserId() {
		return userId;
	}

	public void setUserId(long userId) {
		this.userId = userId;
	}

	public double getRelevance() {
		return relevance;
	}

	public void setRelevance(double relevance) {
		this.relevance = relevance;
	}

	

}
