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

public class ItemDemographic {
	private int demoId;
	private long itemId;
	private double relevance;
	
	public ItemDemographic() {}
	
	public ItemDemographic(int dimId, long itemId, double relevance) {
		super();
		this.demoId = dimId;
		this.itemId = itemId;
		this.relevance = relevance;
	}

	public int getDemoId() {
		return demoId;
	}

	public void setDemoId(int demoId) {
		this.demoId = demoId;
	}

	public long getItemId() {
		return itemId;
	}

	public void setItemId(long itemId) {
		this.itemId = itemId;
	}

	public double getRelevance() {
		return relevance;
	}

	public void setRelevance(double relevance) {
		this.relevance = relevance;
	}

	

}
