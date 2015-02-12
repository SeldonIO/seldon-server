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

package io.seldon.mahout;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.seldon.general.ItemPeer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import io.seldon.api.Constants;
import io.seldon.general.Opinion;
import io.seldon.general.OpinionPeer;
import io.seldon.trust.impl.Recommendation;
import io.seldon.util.CollectionTools;

/**
 * ALS Matrix Factorization recommender using output from <a href="https://issues.apache.org/jira/browse/MAHOUT-542">Mahout-542</a>
 * <p/>
 * Loads matrix into memory
 * <p/>
 * Provides basic recommendations via dot product of user and item vectors 
 * @author rummble
 *
 */
public class ALSRecommender {

	private static Logger logger = Logger.getLogger(ALSRecommender.class.getName());
	
	Map<Long,DenseVector> userFeatures = new HashMap<Long,DenseVector>();
	Map<Long,DenseVector> itemFeatures = new HashMap<Long,DenseVector>();
	
	
	public ALSRecommender(String userDir,String itemDir)
	{
		load(userDir,itemDir);
	}
	
	private void load(String filename,Map<Long,DenseVector> map)
	{
		try 
		{  
		    final Configuration conf = new Configuration();  
		    final FileSystem fs = FileSystem.get(conf);  
		    final SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(filename), conf);  

		    IntWritable key = new IntWritable();  
		    VectorWritable vec = new VectorWritable();  
		    while (reader.next(key, vec)) 
		    {  
		    	System.out.println("key " + key);  
		    	DenseVector vect = (DenseVector)vec.get();

		        map.put((long)key.get(), vect);
		    }  
		       
		    reader.close();  
		         
		} catch(Exception ex){  
			ex.printStackTrace();  
		}  
	}
	
	private void loadVectorFiles(String path,Map<Long,DenseVector> map)
	{
		File dir = new File(path);
		if (!dir.isDirectory())
		{
			System.out.println(path + " is not a directory");
		}
		else
		{
			String[] files = dir.list();
			for(int i=0;i<files.length;i++)
			{
				load(path+"/"+files[i],map);
			}
		}
	}
	
	private void load(String userDir,String itemDir)
	{
		logger.info("Loading mahout ALS user features from " + userDir);
		userFeatures = new HashMap<Long,DenseVector>();
		loadVectorFiles(userDir,userFeatures);
		logger.info("Loading mahout ALS item features from " + itemDir);
		itemFeatures = new HashMap<Long,DenseVector>();
		loadVectorFiles(itemDir,itemFeatures);
	}
	
	public List<Recommendation> recommend(long user,int dimension,int numRecommendations,OpinionPeer opPeer,ItemPeer iPeer)
	{
		Collection<Opinion> ops = opPeer.getUserOpinions(user, 10000);
		Set<Long> exclusions = new HashSet<Long>();
		for(Opinion op : ops)
			exclusions.add(op.getItemId());
		Map<Long,Double> best = new HashMap<Long,Double>(itemFeatures.size());
		boolean checkDimension = !(dimension == Constants.DEFAULT_DIMENSION || dimension == Constants.NO_TRUST_DIMENSION);
		for(Long id : itemFeatures.keySet())
		{
			if (checkDimension && !iPeer.getItemDimensions(id).contains(dimension))
				continue;
			if (!exclusions.contains(id))
			{
				double prediction = predict(user,id);
				logger.info("item:"+id + " predict:"+prediction);
			}
		}
		List<Long> recIds = CollectionTools.sortMapAndLimitToList(best, numRecommendations);
		List<Recommendation> recs = new ArrayList<Recommendation>();
		for(Long id : recIds)
			recs.add(new Recommendation(id,dimension,null));
		return recs;
	}
	
	public Recommendation getPrediction(long user,int type,long item)
	{
		return new Recommendation(item,type,predict(user,item),null,null,null);
	}
	
	private double predict(long user,long item)
	{
		return userFeatures.get(user).dot(itemFeatures.get(item));
	}
	
}
