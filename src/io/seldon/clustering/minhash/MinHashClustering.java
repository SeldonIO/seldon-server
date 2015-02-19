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

package io.seldon.clustering.minhash;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.mahout.clustering.minhash.HashFunction;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.MurmurHash;

import io.seldon.util.CollectionTools;

/**
 * Clustering based on Min Hashing for fast similarity match
 * <p/>
 * Minhash clustering as described @link http://knol.google.com/k/simple-simhashing
 * Assumes we don't have global frequency for items so uses the method described in link to increase clustering precision
 * <p/>
 * The code can create new min hashes as well as use existing ones. It uses Murmur Hash from mahour as the hashing algorithm
 * <p/>
 * Can use a global frequency file to limit hashes to most infrequent items to promote better precision.
 * Can create hashes as either
 * <ul>
 * <li> the first N min hashes 
 * <li> N minhashes from the array of M minhash functions, N<M
 * <li> The number of hash functions is configurable
 * </ul>
 * @author rummble
 *
 */
public class MinHashClustering {

	private static Logger logger = Logger.getLogger(MinHashClustering.class.getName());
	
	private static int numHashFunctions;
	private static HashFunction[] hashFunction;
	private ItemFrequencyHandler itemFreq = null;
	private int cutoffFreq = 75;
	private int maxPrefixSize = 500;

	

	private StoredClusterHandler clusterPeer;
	int numItems = 1;
	int numKeyGroups = 1;
	private int[][] minHashValues;
	
	public static void initialise(boolean create,int numHashFunctions,MinHashSeedStore seedStore)
	{
		logger.info("initialising with " + numHashFunctions + " num hash functions");
		MinHashClustering.numHashFunctions = numHashFunctions;
		hashFunction = new HashFunction[numHashFunctions];
		Random seeder = RandomUtils.getRandom();
		
		for(int i=0;i<numHashFunctions;i++)
		{
			if (create)
			{
				int seed = seeder.nextInt();
				logger.info("Created hash with seed " + seed);
				hashFunction[i] = new MurmurHashWrapper(seed);
				seedStore.storeSeed(i,seed);
			}
			else
			{
				int seed = seedStore.getSeed(i);
				logger.info("Loaded hash with seed " + seed);
				hashFunction[i] = new MurmurHashWrapper(seed);
			}
		}
	}
	
	
	
	static class MurmurHashWrapper implements HashFunction {
	    private final int seed;

	    MurmurHashWrapper(int seed) {
	      this.seed = seed;
	    }

	    @Override
	    public int hash(byte[] bytes) {
	      long hashValue = MurmurHash.hash64A(bytes, seed);
	      return Math.abs((int) (hashValue % RandomUtils.MAX_INT_SMALLER_TWIN_PRIME));
	    }
	  }
	
	
	
	public MinHashClustering(int numItems,int numKeyGroups,StoredClusterHandler clusterPeer)
	{
		this.clusterPeer = clusterPeer;
		this.numItems = numItems;
		this.numKeyGroups = numKeyGroups;
		this.minHashValues = null;
	}

	public void setCutoffFreq(int cutoffFreq) {
		this.cutoffFreq = cutoffFreq;
	}
	
	public void setMaxPrefixSize(int maxPrefixSize) {
		this.maxPrefixSize = maxPrefixSize;
	}
	
	public int getNumKeyGroups() {
		return numKeyGroups;
	}

	public void setNumKeyGroups(int numKeyGroups) {
		this.numKeyGroups = numKeyGroups;
	}

	public void setItemFreq(ItemFrequencyHandler itemFreq) {
		this.itemFreq = itemFreq;
	}
	
	private void createHashes(Collection<String> ids)
	{		
		logger.info("Creating hashes with "+ids.size() + " ids");
		minHashValues = new int[numHashFunctions][numItems];
		for(int i=0;i<numHashFunctions;i++)
			for(int j=0;j<numItems;j++)
				minHashValues[i][j]=Integer.MAX_VALUE;
		for(String id : ids)
		{
			for(int i=0;i<numHashFunctions;i++)
			{
				int hash = hashFunction[i].hash(id.getBytes());
				int j = 0;
				for(;j<minHashValues[i].length;j++)
					if (minHashValues[i][j] > hash)
						break;
				int temp;
				//move hashes down and inserting new hash at front
				for(;j<minHashValues[i].length;j++)
				{
					temp = minHashValues[i][j];
					minHashValues[i][j] = hash;
					hash = temp;
				}
			}
		}
	}
	
	private List<String> frequencyLimit(Set<String> interests)
	{
		Map<String,Long> f = new HashMap<>();
		for(String id : interests)
		{
			long freq = itemFreq.getFrequency(id);
			if (freq < this.cutoffFreq)
				f.put(id, freq );
		}
		return CollectionTools.sortMapAndLimitToList(f, this.maxPrefixSize,false);
	}
	
	private Set<String> getHashes(int maxSharedValues,int maxKeyGroups)
	{
		logger.info("Get hashes with "+numHashFunctions+" num hash functions and "+maxKeyGroups+" max key groups");
		Set<String> hashes = new HashSet<>(numHashFunctions);
		//output MinHashes
		for (int i = 0; i < numHashFunctions; i++) 
		{
			StringBuffer h = new StringBuffer();
			for(int k=0;k<maxKeyGroups;k++)
			{
				for (int j = 0; j < maxSharedValues; j++) 
				{
					h.append(minHashValues[(i+k) % numHashFunctions][j]).append('-');
				}
			}
			String hash = h.toString();
			//hash = hash.substring(0, hash.lastIndexOf('-'));
			hashes.add(hash);
		}
		return hashes;
	}
	
	private void createHashesIfNeeded(Set<String> interests)
	{
		if (minHashValues == null)
		{
			if (itemFreq != null)
				createHashes(frequencyLimit(interests));
			else
				createHashes(interests);
		}
	}
	
	public void storeHashesForUser(long userId,Set<String> interests)
	{
		createHashesIfNeeded(interests);
		//FIXME - only works if either numItems OR numKeyGroups = 1
		Set<String> hashes =  getHashes(this.numItems,this.numKeyGroups);
		logger.info("Storing " + hashes.size() + " hashes");
		clusterPeer.storeHashes(userId, hashes);
	}
	

	public Map<Long,Integer> getSimilarUsers(long userId,Set<String> interests,int minMatches)
	{
		int dbCalls = 0;
		createHashesIfNeeded(interests);
		Map<Long,Integer> matches = new HashMap<>();
		boolean stop = false;
		//assumes numItems or numKeyGroups = 1
		int limit = this.numItems > 1 ? this.numItems : this.numKeyGroups;
		for(int i=limit;i>0;i--)
		{
			Set<String> hashesAti = getHashes(this.numItems > 1 ? i : this.numItems,this.numKeyGroups > 1 ? i : this.numKeyGroups);
			
			// could use Bloom filter here to locally test set membership rather than go to DB
				
			Set<Long> users = clusterPeer.getUsers(hashesAti);
			dbCalls++;
			if (users.size() > 0)
				logger.info("Found " + users.size() + " at hash length " + i);
			for(Long user : users)
				if (!matches.containsKey(user))
					matches.put(user,i);
				
			if (matches.size() > minMatches)
				break;
		}
		//clusterPeer.storeHash(userId, hashes);
		logger.info("did "+dbCalls+" db calls to get "+matches.size()+" matches with minMatches:"+minMatches);
		return matches;
	}
	
	public static void main(String[] args)
	{

	}
}
