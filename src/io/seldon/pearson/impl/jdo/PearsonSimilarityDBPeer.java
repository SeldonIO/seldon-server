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

package io.seldon.pearson.impl.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.pearson.PearsonSimilarityHandler;

public class PearsonSimilarityDBPeer implements PearsonSimilarityHandler {

	PersistenceManager pm;
	
	public PearsonSimilarityDBPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}

    public Double getSimilarity(long user1, long user2) {
		Query query = pm.newQuery("javax.jdo.query.SQL","select p from pearson where m1=? and m2=?");
		query.setUnique(true);
		Double p = (Double) query.execute(user1<user2 ? user1 : user2,user1<user2 ? user2 : user1);
		return p;
	}

    public Set<Long> getNeighbourhood(long content,long user, int k) {
		Query query = pm.newQuery("javax.jdo.query.SQL","select m  from ((select m2 as m,p from pearson where m1=? order by p desc) union (select m1 as m,p from pearson where m2=? order by p desc) order by p desc) t,opinions r where r.item_id=? and r.user_id=t.m order by t.p desc limit ?;");
		ArrayList<Object> args = new ArrayList<>();
		args.add(user);
		args.add(user);
		args.add(content);
		args.add(k);
		Collection<Long> c = (Collection<Long>) query.executeWithArray(args.toArray());
		Set<Long> res = new HashSet<>();
		for(Long l : c)
			res.add(l);
		return res;
	}

}
