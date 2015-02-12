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

package io.seldon.semvec;

import pitt.search.semanticvectors.CompoundVectorBuilder;
import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.LuceneUtils;
import pitt.search.semanticvectors.VectorStore;
import pitt.search.semanticvectors.vectors.Vector;

public class VectorStorePredictor {
	
	private VectorStore queryVecStore;
	private VectorStore searchVecStore;
	private LuceneUtils luceneUtils;
	private FlagConfig flagConfig = FlagConfig.getFlagConfig(null);
	Vector queryVector;
	
	public VectorStorePredictor(String queryTerm,
			VectorStore queryVecStore,
            VectorStore searchVecStore,
            LuceneUtils luceneUtils) 
	{
		this.queryVecStore = queryVecStore;
		this.searchVecStore = searchVecStore;
		this.luceneUtils = luceneUtils;
		
		queryVector = CompoundVectorBuilder.getQueryVectorFromString(queryVecStore,
		        luceneUtils,
		        flagConfig,
		        queryTerm);
	}
	
	public String getPrediction(String searchTerm,String[] suffixes)
	{
		String bestSuffix = null;
		double bestScore = 0;
		if (queryVector != null)
		{
			for(String suffix : suffixes)
			{
				String term = searchTerm +"_"+suffix;
				Vector vec = CompoundVectorBuilder.getQueryVectorFromString(searchVecStore,
				        luceneUtils,
				        flagConfig,
				        term);
				if (vec != null)
				{
					// old SV 2.2 code
					//float simScore = VectorUtils.scalarProduct(queryVector, vec);
					double simScore = queryVector.measureOverlap(vec);
					if (simScore > bestScore)
					{
						bestSuffix = suffix;
						bestScore = simScore;
					}
				}
			}
		}
		if (bestScore > 0.15)
			return bestSuffix;
		else 
			return null;
	}
	
}
