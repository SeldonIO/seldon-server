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

package io.seldon.elph.jdo;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jdo.Query;

import io.seldon.db.jdo.ClientPersistable;
import io.seldon.elph.IElphPredictionPeer;
import org.apache.log4j.Logger;

import io.seldon.util.CollectionTools;

public class SqlElphPredictionPeer  extends ClientPersistable implements IElphPredictionPeer {

	private static Logger logger = Logger.getLogger(SqlElphPredictionPeer.class.getName());
	
	public SqlElphPredictionPeer(String clientName) {
		super(clientName);
		// TODO Auto-generated constructor stub
	}

	/**
	 * sorted must be a list of sorted unique item ids
	 */
	@Override
	public Map<Long, Double> getPredictions(List<Long> sorted,int dimension) {
		String queryStr = CollectionTools.join(sorted, " ");
		String sql = "select e.item_id,match(hypothesis) against (? in boolean mode) as score,c/entropy as score2 from elph_hypothesis e join item_map_enum ime on (e.item_id=ime.item_id) join dimension d on (d.dim_id="+dimension+" and d.attr_id=ime.attr_id and d.value_id=ime.value_id) where match(hypothesis) against (? in boolean mode) order by score desc,c/entropy desc";
		logger.info("About to run "+sql+" with "+queryStr);
		Query query = getPM().newQuery( "javax.jdo.query.SQL", sql);
		Collection<Object[]> results = (Collection<Object[]>) query.execute(queryStr,queryStr);
		logger.info("Got "+results.size()+" results");
		Map<Long,Double> scores = new HashMap<Long,Double>();
		double topScore1 = -1;
		double topScore2 = -1;
		double lastScore1 = -1; 
		for(Object[] res : results)
		{
			Long itemId = (Long) res[0];
			if (scores.containsKey(itemId))
				continue;
			Double score1 = (Double) res[1];
			Double score2 = (Double) res[2];
			if (topScore1 == -1)
			{
				topScore1 = score1;
				topScore2 = score2;
			}
			else
			{
				if (score1 != lastScore1)
				{
					topScore2 = score2;
				}
			}
			lastScore1 = score1;
			double score = score1/topScore1 * score2/topScore2;
			logger.info("Adding "+itemId+"->"+score);
			scores.put(itemId, score);
		}
		
		query.closeAll();
		return scores;
	}

	

}
