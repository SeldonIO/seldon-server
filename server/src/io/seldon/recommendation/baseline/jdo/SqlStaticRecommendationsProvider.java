/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.recommendation.baseline.jdo;

import io.seldon.api.Constants;
import io.seldon.db.jdo.ClientPersistable;
import io.seldon.recommendation.baseline.IStaticRecommendationsProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jdo.Query;

import org.apache.commons.lang.StringUtils;

public class SqlStaticRecommendationsProvider extends ClientPersistable implements IStaticRecommendationsProvider {

	public SqlStaticRecommendationsProvider(String clientName) {
		super(clientName);
	}

	@Override
	public Map<Long, Double> getStaticRecommendations(long userId,Set<Integer> dimensions, int max) {
		Collection<Object[]> results;
		Query query;
		if (dimensions.isEmpty() || (dimensions.size() == 1 && dimensions.iterator().next() == Constants.DEFAULT_DIMENSION))
		{
			String sql = "select item_id,score from recommendations where user_id=? order by score desc";
			if (max > 0)
				sql = sql + " limit "+max;
			query = getPM().newQuery( "javax.jdo.query.SQL", sql);
			ArrayList<Object> args = new ArrayList<>();
			args.add(userId);
			results = (Collection<Object[]>) query.executeWithArray(args.toArray());
		}
		else
		{
			String dimensionsStr = StringUtils.join(dimensions, ",");
			String sql = "select recs.item_id,score from recommendations recs join item_map_enum ime on (recs.item_id=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id) where dim_id in ("+dimensionsStr+") and recs.user_id=? order by score desc";
			if (max > 0)
				sql = sql + " limit "+max;
			query = getPM().newQuery( "javax.jdo.query.SQL", sql);
			ArrayList<Object> args = new ArrayList<>();
			args.add(userId);
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

	
}
