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

package io.seldon.similarity.item;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.Query;

import io.seldon.db.jdo.ClientPersistable;
import org.apache.log4j.Logger;

import io.seldon.api.Constants;

public class JdoItemSimilarityPeer extends ClientPersistable implements IItemSimilarityPeer {
	private static Logger logger = Logger.getLogger( JdoItemSimilarityPeer.class.getName() );

	public JdoItemSimilarityPeer(String clientName) {
		super(clientName);
	}

	@Override
	public Map<Long, Double> getSimilarItems(long itemId,int dimension,int max) {
		Collection<Object[]> results;
		Query query;
		if(dimension == Constants.DEFAULT_DIMENSION)
		{
			String sql = "select isim.item_id2 as item_id,score from item_similarity isim where isim.item_id=? union select isim.item_id,score from item_similarity isim where isim.item_id2=? order by score desc";
			if (max > 0)
				sql = sql + " limit "+max;
			query = getPM().newQuery( "javax.jdo.query.SQL", sql);
			ArrayList<Object> args = new ArrayList<>();
			args.add(itemId);
			args.add(itemId);
			results = (Collection<Object[]>) query.executeWithArray(args.toArray());
		}
		else
		{
			String sql = "select isim.item_id2 as item_id,score from item_similarity isim join item_map_enum ime on (isim.item_id2=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id) where dim_id=? and isim.item_id=? union select isim.item_id,score from item_similarity isim join item_map_enum ime on (isim.item_id=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id) where dim_id=? and isim.item_id2=? order by score desc";
			if (max > 0)
				sql = sql + " limit "+max;
			query = getPM().newQuery( "javax.jdo.query.SQL", sql);
			ArrayList<Object> args = new ArrayList<>();
			args.add(dimension);
			args.add(itemId);
			args.add(dimension);
			args.add(itemId);
			results = (Collection<Object[]>) query.executeWithArray(args.toArray());
		}
		Map<Long,Double> map = new HashMap<>();
		for(Object[] res : results)
		{
			Long item = (Long) res[0];
			Double score = (Double) res[1];
			map.put(item, score);
		}
		query.closeAll();
		return map;
	}

	@Override
	public Map<Long, Double> getRecommendations(long userId, int dimension, int max) {
		String sql = "select r.item_id,score from recommendations r join item_map_enum ime on (r.item_id=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id) where dim_id=? and user_id=? order by score desc";
		if (max > 0)
			sql = sql + " limit "+max;
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute(dimension,userId);
		Map<Long,Double> map = new HashMap<>();
		for(Object[] res : results)
		{
			Long item = (Long) res[0];
			Double score = (Double) res[1];
			map.put(item, score);
		}
		query.closeAll();
		return map;
	}

	
}
