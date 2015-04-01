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

package io.seldon.recommendation.baseline.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jdo.Query;

import io.seldon.db.jdo.ClientPersistable;
import io.seldon.recommendation.baseline.IBaselineRecommenderUtils;
import org.apache.commons.lang.StringUtils;

public class SqlBaselineRecommenderUtils  extends ClientPersistable implements IBaselineRecommenderUtils {

	public SqlBaselineRecommenderUtils(String clientName) {
		super(clientName);
	}

	@Override
	public Map<Long, Double> getPopularItems(int dimension, int numRecommendations) {
		
		String sql = "select ip.item_id,opsum from items_popular ip join item_map_enum ime on (ip.item_id=ime.item_id) join dimension d on (ime.attr_id=d.attr_id and ime.value_id=d.value_id) where dim_id=? order by opsum desc limit "+numRecommendations;
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		ArrayList<Object> args = new ArrayList<>();
		args.add(dimension);
		Collection<Object[]> results = (Collection<Object[]>) query.executeWithArray(args.toArray());
		Map<Long,Double> map = new HashMap<>();
		Double topScore = null;
		for(Object[] res : results)
		{
			if (topScore == null)
				topScore = (Double) res[1];
			Long item = (Long) res[0];
			Double score = (Double) res[1];
			map.put(item, score/topScore);
		}
		query.closeAll();
		return map;
	}

	@Override
	public Map<Long, Double> reorderByMostPopular(Set<Long> itemIds) {
		String idStr = StringUtils.join(itemIds, ",");
		String sql = "select ip.item_id,opsum from items_popular where item_id in ("+idStr+") order by opsum desc";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		Map<Long,Double> map = new HashMap<>();
		Double topScore = null;
		for(Object[] res : results)
		{
			if (topScore == null)
				topScore = (Double) res[1];
			Long item = (Long) res[0];
			Double score = (Double) res[1];
			map.put(item, score/topScore);
		}
		query.closeAll();
		return map;
	}

	@Override
	public Map<Long, Double> getAllItemPopularity() {
		String sql = "select item_id,opsum from items_popular order by opsum desc";
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute();
		Map<Long,Double> map = new HashMap<>();
		Double topScore = null;
		for(Object[] res : results)
		{
			if (topScore == null)
				topScore = (Double) res[1];
			Long item = (Long) res[0];
			Double score = (Double) res[1];
			map.put(item, score/topScore);
		}
		query.closeAll();
		return map;
	}

	

}
