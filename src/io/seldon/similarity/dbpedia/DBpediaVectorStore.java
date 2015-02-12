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

package io.seldon.similarity.dbpedia;

import java.io.IOException;

import pitt.search.semanticvectors.CompoundVectorBuilder;
import pitt.search.semanticvectors.FlagConfig;
import pitt.search.semanticvectors.VectorStoreRAM;
import pitt.search.semanticvectors.vectors.Vector;

public class DBpediaVectorStore {

	private static DBpediaVectorStore store;
	private FlagConfig flagConfig = FlagConfig.getFlagConfig(null);
	public static DBpediaVectorStore initialise(String termVectorPath) throws IOException
	{
		store = new DBpediaVectorStore(termVectorPath);
		return store;
	}
	
	public static DBpediaVectorStore get()
	{
		return store;
	}
	
	VectorStoreRAM vecstore;
	
	private DBpediaVectorStore(String termVectorPath) throws IOException
	{
		vecstore = new VectorStoreRAM(flagConfig);
		vecstore.initFromFile(termVectorPath);
	}
	
	public double compare(Vector vector,String query2)
	{
		Vector v2 = getVector(query2);
		if (v2 == null|| v2.isZeroVector()) return 0;
		return vector.measureOverlap(v2);
	}
	
	public Vector getVector(String query)
	{
		return CompoundVectorBuilder.getQueryVectorFromString(vecstore,null,flagConfig,query);
	}
	
	
}
