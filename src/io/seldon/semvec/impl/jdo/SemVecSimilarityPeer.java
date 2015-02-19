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

package io.seldon.semvec.impl.jdo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;

import io.seldon.semvec.SemanticVectorSimilarity;
import io.seldon.trust.impl.SearchResult;

public class SemVecSimilarityPeer implements SemanticVectorSimilarity {

	PersistenceManager pm;
	
	public SemVecSimilarityPeer(PersistenceManager pm)
	{
		this.pm = pm;
	}
	
	public ArrayList<SearchResult> getSimilar(long content)
	{
		ArrayList<SearchResult> res = new ArrayList<>();
		Query query = pm.newQuery("javax.jdo.query.SQL","select dst,similarity from content_similarity where src=? order by similarity desc");
		Collection c = (Collection) query.execute(content);
		for(Iterator i=c.iterator();i.hasNext();)
		{
			Object[] rs = (Object[]) i.next();
			Long dst = (Long)rs[0];
			Double sim = (Double)rs[1];
			if (!dst.equals(content)) {
				SearchResult s = new SearchResult(dst,sim);
				res.add(s);
			}
		}
		query.closeAll();
		return res;
	}
	
}
