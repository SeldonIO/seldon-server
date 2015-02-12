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

package io.seldon.similarity.dbpedia;

import java.util.Collection;
import java.util.List;

import io.seldon.api.resource.ItemBean;

/**
 * Store for web searches against lucene dbpedia index
 * @author rummble
 *
 */
public interface WebSearchResultsPeer {

	public Integer getHits(long itemId);
	public List<DBpediaItemSearch> getHitsInRange(int hits, int oom);
	public List<DBpediaItemSearch> getHitsInRange(int hits, long minItemId, int oom);
	public List<DBpediaItemSearch> getHitsInRange(long userId,String linkType,int hits,int oom);
	public List<DBpediaItemSearch> getHitsForUser(long userId,String linkType);
	public void storeNewItems(Collection<ItemBean> items);
}
