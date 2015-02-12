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

package io.seldon.mahout.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import org.apache.log4j.Logger;

import io.seldon.mahout.FPGrowthStore;
import io.seldon.util.CollectionTools;

public class FPGrowthPeer implements FPGrowthStore {

	private static Logger logger = Logger.getLogger(FPGrowthPeer.class.getName());

	PersistenceManager pm;
	
	public FPGrowthPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	/**
	 * Uses lift as ordering. At present does not filter out super-set rules, e.g. (3,671)->(4) and (3)->(4)
	 * 
	 * @param items
	 * @return
	 */
	private List<Long> matchAssociationRules(Collection<Long> items)
	{
		String iSet = CollectionTools.join(items, ",");
		Query query = pm.newQuery( "javax.jdo.query.SQL", "select consequent from assoc_rules natural join (select * from assoc_rules where item in ("+iSet+") group by set_id,consequent having count(*)=size) a where consequent not in ("+iSet+") group by consequent order by sum(lift) desc");
		Collection<Long> rows = (Collection<Long>) query.execute(items.size());
		return new ArrayList<Long>(rows);
	}
	
	

	public List<Long> getSupportedItems(List<Long> transaction,int numResults) {
		return matchAssociationRules(transaction);
	}
	
	
	/**
	 * Finds all consequents that are in items produced by the rules that match the transaction list of items
	 * @param transactions
	 * @param items
	 * @return
	 */
	//FIXME - need to divide by antecent support
	private List<Long> sortByAssociationRules(List<Long> transactions,Collection<Long> items)
	{
		String tSet = CollectionTools.join(transactions, ",");
		String iSet = CollectionTools.join(items, ",");
		String sql = "select consequent from assoc_rules natural join (select * from assoc_rules where item in ("+tSet+") group by set_id,consequent having count(*)=size) a where consequent in ("+iSet+") group by consequent order by sum(lift) desc";
		Query query = pm.newQuery( "javax.jdo.query.SQL", sql);
		Collection<Long> rows = (Collection<Long>) query.execute();
		return new ArrayList<Long>(rows);
		//
	}

	public List<Long> getOrderedItems(List<Long> transactions, Collection<Long> items) {
		return sortByAssociationRules(transactions,items);
	}
	
}
