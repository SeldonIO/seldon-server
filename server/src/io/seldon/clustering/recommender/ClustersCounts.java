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

package io.seldon.clustering.recommender;

import java.io.Serializable;
import java.util.Map;

public class ClustersCounts implements Serializable {

	Map<Long,Double> itemCounts;
	long timestamp;
	
	public ClustersCounts(Map<Long, Double> itemCounts, long timestamp) {
		super();
		this.itemCounts = itemCounts;
		this.timestamp = timestamp;
	}
	public Map<Long, Double> getItemCounts() {
		return itemCounts;
	}
	public void setItemCounts(Map<Long, Double> itemCounts) {
		this.itemCounts = itemCounts;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
