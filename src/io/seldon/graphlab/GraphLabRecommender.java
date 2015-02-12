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

package io.seldon.graphlab;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.general.ItemPeer;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.TreeBidiMap;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseMatrix;

import io.seldon.api.Constants;
import io.seldon.general.Opinion;
import io.seldon.general.OpinionPeer;
import io.seldon.trust.impl.Recommendation;
import io.seldon.util.CollectionTools;

/**
 * GraphLab  Recommender
 * <p/>
 * See <a href="http://wiki.rummble.com/GraphLab">Graph Lab wiki page</a>
 * <p/>
 * Loads binaryoutput matrix file and mappings file on creation.
 * The mappings file defines the mapping of user and item ids in graphlab to those in our API
 * <p/>
 * Provides basic recommendation using dot product on the vectors for users and items
 * @author rummble
 *
 */
public class GraphLabRecommender {

	private static Logger logger = Logger.getLogger(GraphLabRecommender.class.getName());
	
	BidiMap itemIdMap;
	BidiMap userIdMap;
	DenseMatrix U;
	DenseMatrix V;
	DenseMatrix K = null;
	int nUsers;
	int nItems;
	int nTimeBins;
	int dimension;
	
	public GraphLabRecommender(String matrixFilename,String mappingFilename) throws IOException
	{
		loadMatrices(matrixFilename);
		loadMappings(mappingFilename);
	}
	
	private void loadMappings(String filename) throws IOException
	{
		
		BidiMap map = null;
		File file = new File(filename);
		
		if (file.exists())
		{
			userIdMap = new TreeBidiMap();
			itemIdMap = new TreeBidiMap();
			BufferedReader input =  new BufferedReader(new FileReader(file));
			try 
			{
				String line = null; //not declared within while loop
				long count = 0;
		        while (( line = input.readLine()) != null)
		        {
		        	if (line.startsWith("#"))
		        	{
		        		if (line.indexOf("xrow") > 0)
		        		{
		        			count = 0;
		        			map = userIdMap;
		        		}
		        		else if (line.indexOf("xcol") > 0)
		        		{
		        			count = 0;
		        			map = itemIdMap;
		        		}
		        	}
		        	else
		        	{
		        		count++; // first id must be 1
		        		int graphId = Integer.parseInt(line.trim());
		        		if (graphId > 0)
		        			map.put(count, graphId);

		        	}
		        }
			}
			finally
			{
				input.close();
			}
		}
		else
			logger.info("no mapping file " + filename + " found");
	}
	
	/*
	 * Read a C++ stored integer. Assumes little endian input.
	 */
	private int readInt(DataInputStream in) throws IOException
	{
		byte[] buffer = new byte[4];
		buffer[0] = in.readByte();
		buffer[1] = in.readByte();
		buffer[2] = in.readByte();
		buffer[3] = in.readByte();
		int nextInt = (buffer[0] & 0xFF) | (buffer[1] & 0xFF) << 8 | (buffer[2] & 0xFF) << 16 | (buffer[3] & 0xFF) << 24;
		return nextInt;
	}
	
	/**
	 * Read C++ stored double. 
	 * @param in
	 * @return
	 * @throws IOException
	 */
	private double readDouble(DataInputStream in) throws IOException
	{
		long bits = 0;
		for (int i = 0; i < 64; i += 8)
			bits += (long)in.read() << i;
		double answer = Double.longBitsToDouble(bits);
		return answer;
	}
	
	
	
	/**
	 * OUTPUT: SAVE FACTORS U,V,T to a binary file
	 * FORMAT: M N K D (4 x ints = user, movies, time bins, feature width (dimension))
	 * MATRIX U ( M x D doubles)
	 * MATRIX V ( N x D doubles)
	 * MATRIX K ( K x D doubles - optional, only for tensor)
	 * TOTAL FILE SIZE: 4 ints + (M+N+K)*D - for tensor
	 * 4 ints + (M+N)*D - for matrix 
	 * @throws IOException 
	 */
	private void loadMatrices(String filename) throws IOException
	{
		DataInputStream in = new DataInputStream(new BufferedInputStream(
                new FileInputStream(filename)));
		nUsers = readInt(in);
		nItems = readInt(in);
		nTimeBins = readInt(in);
		dimension = readInt(in);
		U = new DenseMatrix(nUsers,dimension);
		V  = new DenseMatrix(nItems,dimension);
		if (nTimeBins > 1)
			K = new DenseMatrix(nTimeBins,dimension);
		logger.info("loading " + filename + " with nUsers "  + nUsers + " nItems " + nItems+ " nTimeBins:" + nTimeBins + " d "+dimension);
		for(int column=0;column<dimension;column++)
			for(int row=0;row<nUsers;row++)
				U.setQuick(row, column, readDouble(in));
		for(int column=0;column<dimension;column++)
			for(int row=0;row<nItems;row++)
				V.setQuick(row, column, readDouble(in));
		if (nTimeBins > 1)
			for(int column=0;column<dimension;column++)
				for(int row=0;row<nTimeBins;row++)
					K.setQuick(row, column, readDouble(in));
	}
	
	private Integer getGraphIdFromLabsId(BidiMap map,long id)
	{
		if (map != null)
		{
			Integer gid = (Integer) map.get(id);
			if (gid != null)
				return gid-1; // remove 1 to get matrix id starting at zero. Graphlab ids start at 1
			else
			{
				logger.warn("Can't get graphlab id for labs id " + id);
				return null;
			}
		}
		else
			return (int) id-1;
	}
	
	private Long getLabsIdFromGraphId(BidiMap map,int id)
	{
		// matrix ids start at zero but mapping from graphlab starts at 1 so add 1 before getting from map
		if (map != null)
		{
			Long labsId = (Long) map.getKey(id+1);
			if (labsId != null)
				return labsId;
			else
				return null;
			
		}
		else
			return new Long(id+1);
	}
	
	public Recommendation getPrediction(long user,int type,long item)
	{
		Integer userIdGL = getGraphIdFromLabsId(userIdMap,user);
		Integer itemIdGL = getGraphIdFromLabsId(itemIdMap,item);
		if (userIdGL != null && itemIdGL != null)
			return new Recommendation(item,type,predict(userIdGL,itemIdGL),null,null,null);
		else
		{
			if (userIdGL == null)
				logger.warn("Can't find graphlab id for user " + user);
			if (itemIdGL == null)
				logger.warn("Can't find graphlab id for item " + item);
			return null;
		}
	}
	
	private double predict(int userIdGL,int itemIdGL)
	{
	//	if (K == null)

			return U.viewRow((int)userIdGL).dot(V.viewRow((int)itemIdGL));
	//	else
	//		return U.getRow((int)user).dot(V.getRow((int)item));
	}
	
	//FIXME hardwired to assume mapping of GL ids are 1 larger than actual as rows and columns start from 0
	public List<Recommendation> recommend(long user,int dimension,int numRecommendations,OpinionPeer opPeer,ItemPeer iPeer)
	{
		Collection<Opinion> ops = opPeer.getUserOpinions(user, 10000);
		Set<Long> exclusions = new HashSet<Long>();
		for(Opinion op : ops)
			exclusions.add(op.getItemId());
		Map<Long,Double> best = new HashMap<Long,Double>(V.numRows());
		Integer userIdGL = getGraphIdFromLabsId(userIdMap,user);
		logger.info("Translated user id " + user + " to " + userIdGL);
		boolean checkDimension = !(dimension == Constants.DEFAULT_DIMENSION || dimension == Constants.NO_TRUST_DIMENSION);
		if (userIdGL != null)
		{
			for(int id=0;id<V.numRows();id++)
			{
				long itemId = getLabsIdFromGraphId(itemIdMap,id);
				if (checkDimension && !iPeer.getItemDimensions(itemId).contains(dimension))
					continue;
				if (!exclusions.contains(itemId))
				{
					double prediction = predict(userIdGL,id);
					best.put(itemId, prediction);
				}
			}
			List<Long> recIds = CollectionTools.sortMapAndLimitToList(best, numRecommendations);
			List<Recommendation> recs = new ArrayList<Recommendation>();
			for(Long id : recIds)
			{
				recs.add(new Recommendation(id,dimension,null));
			}
			return recs;
		}
		else
			return null;
	}
}
