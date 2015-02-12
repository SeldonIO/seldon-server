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

package io.seldon.bayes.impl.jdo;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.bayes.BayesDataProvider;

/**
 * Provide a JDO impl of data needed for a naive bayesian predictor.
 * Uses raw SQL rather than JDOQL
 * @author Clive
 *
 */
public class BayesDBPeer implements BayesDataProvider {
	PersistenceManager pm;
	
	public BayesDBPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	public Double getPrGivenU(long user,int rating)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select pru from pr_u where user=? and rating=?");
		query.setUnique(true);
		Double avg = (Double) query.execute(user,rating);
		return avg;
	}
	
	public void fillPrGivenU(Map<String,Double> map)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select user,rating,pru from pr_u");
		Collection c = (Collection) query.execute();
		for(Iterator i=c.iterator();i.hasNext();)
		{
			Object[] res = (Object[]) i.next();
			Long user = (Long) res[0];
			Integer rating = (Integer) res[1];
			Double pr = (Double) res[2];
			map.put(""+user+":"+rating, pr);
		}
	}
	
	public Double getPrGivenI(long content,int rating)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select pri from pr_i where content=? and rating=?");
		query.setUnique(true);
		Double avg = (Double) query.execute(content,rating);
		return avg;
	}
	
	public void fillPrGivenI(Map<String,Double> map)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select content,rating,pri from pr_i");
		Collection c = (Collection) query.execute();
		for(Iterator i=c.iterator();i.hasNext();)
		{
			Object[] res = (Object[]) i.next();
			Long content = (Long) res[0];
			Integer rating = (Integer) res[1];
			Double pr = (Double) res[2];
			map.put(""+content+":"+rating, pr);
		}
	}
	
	public Double getPr(int rating)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select pr from pr where rating=?");
		query.setUnique(true);
		Double avg = (Double) query.execute(rating);
		return avg;
	}
	
	public void fillPr(Map<Integer,Double> map)
	{
		Query query = pm.newQuery("javax.jdo.query.SQL","select rating,pr from pr");
		Collection c = (Collection) query.execute();
		for(Iterator i=c.iterator();i.hasNext();)
		{
			Object[] res = (Object[]) i.next();
			Integer rating = (Integer) res[0];
			Double pr = (Double) res[1];
			map.put(rating, pr);
		}
	}
	
	
}
