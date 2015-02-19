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

package io.seldon.similarity.vspace.jdo;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.similarity.vspace.TagStore;

public class TagStorePeer implements TagStore {

	PersistenceManager pm;
	
	public TagStorePeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	@Override
	public double getIDF(String tag) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<String, Long> getItemTags(long itemId) {
		Query query = pm.newQuery("javax.jdo.query.SQL", "select tag,tf from item_tag where item_id="+itemId);
		Collection<Object[]> c = (Collection<Object[]>) query.execute();
		Map<String,Long> tags = new HashMap<>();
		for(Object[] r : c)
		{
			long val = (Integer)r[1];
			tags.put((String)r[0], val);
		}
		return tags;
	}

	@Override
	public Map<String, Long> getUserTags(long userId) {
		Query query = pm.newQuery("javax.jdo.query.SQL", "select tag,tf from user_tag where user_Id="+userId);
		Collection<Object[]> c = (Collection<Object[]>) query.execute();
		Map<String,Long> tags = new HashMap<>();
		for(Object[] r : c)
		{
			long val = (Integer)r[1];
			tags.put((String)r[0], val);
		}
		return tags;
	}

}
