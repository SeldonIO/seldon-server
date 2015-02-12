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

package io.seldon.clustering.tag.jdo;

import java.io.Serializable;
import java.util.Set;

public class ItemTags implements Serializable {

	long itemId;
	Set<String> tags;
	long lastCheckTimestamp;
	public ItemTags(long itemId, Set<String> tags, long lastCheckTimestamp) {
		super();
		this.itemId = itemId;
		this.tags = tags;
		this.lastCheckTimestamp = lastCheckTimestamp;
	}
	public long getItemId() {
		return itemId;
	}
	public void setItemId(long itemId) {
		this.itemId = itemId;
	}
	public Set<String> getTags() {
		return tags;
	}
	public void setTags(Set<String> tags) {
		this.tags = tags;
	}
	public long getLastCheckTimestamp() {
		return lastCheckTimestamp;
	}
	public void setLastCheckTimestamp(long lastCheckTimestamp) {
		this.lastCheckTimestamp = lastCheckTimestamp;
	}
	
	

}
