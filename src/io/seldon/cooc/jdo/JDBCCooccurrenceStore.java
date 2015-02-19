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

package io.seldon.cooc.jdo;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.cooc.CooccurrenceCount;
import io.seldon.cooc.CooccurrencePeer;
import io.seldon.cooc.ICooccurrenceStore;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.util.CollectionTools;

public class JDBCCooccurrenceStore extends ClientPersistable implements ICooccurrenceStore {

	public JDBCCooccurrenceStore(String clientName) {
		super(clientName);
	}

	@Override
	public Map<String, CooccurrenceCount> getCounts(List<Long> item1,List<Long> item2) {
		Map<String,CooccurrenceCount> map = new HashMap<>();
		if (item1 == null || item1.size() == 0)
			return map;
		if (item2 == null || item2.size() == 0)
			return map;
		final PersistenceManager pm = getPM();
		String sql = "select item_id1,item_id2,count,time from cooc_counts where item_id1 in ("+CollectionTools.join(item1, ",")+") and item_id2 in ("+CollectionTools.join(item2, ",")+")";
		Query query = pm.newQuery("javax.jdo.query.SQL",sql);
		Collection<Object[]> res = (Collection<Object[]>)  query.execute();
		for(Object[] r : res)
		{
			Long i1 = (Long) r[0];
			Long i2 = (Long) r[1];
			Double count = (Double) r[2];
			Long time = (Long) r[3];
			String key = CooccurrencePeer.getKey(i1, i2);
			map.put(key, new CooccurrenceCount(count, time));
		}
		return map;
	}

	@Override
	public Map<String, CooccurrenceCount> getCounts(List<Long> item1) {
		Map<String,CooccurrenceCount> map = new HashMap<>();
		if (item1 == null || item1.size() == 0)
			return map;
		final PersistenceManager pm = getPM();
		String sql = "select item_id1,item_id2,count,time from cooc_counts where item_id1 in ("+CollectionTools.join(item1, ",")+") and item_id2=item_id1";
		Query query = pm.newQuery("javax.jdo.query.SQL",sql);
		Collection<Object[]> res = (Collection<Object[]>)  query.execute();
		for(Object[] r : res)
		{
			Long i1 = (Long) r[0];
			Long i2 = (Long) r[1];
			Double count = (Double) r[2];
			Long time = (Long) r[3];
			String key = CooccurrencePeer.getKey(i1, i2);
			map.put(key, new CooccurrenceCount(count, time));
		}
		return map;
	}

}
