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
import java.util.Properties;
import java.util.Set;

import io.seldon.general.ActionPeer;
import io.seldon.general.UserPeer;
import io.seldon.trust.impl.TrustNetworkSupplier;
import org.apache.log4j.Logger;

import io.seldon.general.Action;
import io.seldon.general.Opinion;
import io.seldon.general.OpinionPeer;
import io.seldon.general.User;
import io.seldon.trust.impl.RecommendationNetwork;
import io.seldon.trust.impl.Trust;
import io.seldon.util.CollectionTools;

/**
 * Peer to 
 * <ul>
 * <li> create min hashes for all users in system (bootstrap)
 * <li> create a trust network using min hashing for a user
 * <li> test minhashing
 * </ul>
 * <a href="http://wiki.rummble.com/FacebookProject">wiki page</a>
 * @author rummble
 *
 */
public class MinHashClusterPeer {
	private static Logger logger = Logger.getLogger(MinHashClusterPeer.class.getName());
	MinHashSeedStore seedStore;
	StoredClusterHandler clusterStore;
	
	public static int maxSharedInterests = 10;
	public static int numKeyGroups = 1;
	
	public static void initialise(Properties props)
	{
		String maxSharedInterestsStr = props.getProperty("io.seldon.minhash.maxsharedinterests");
		String numKeyGroupsStr = props.getProperty("io.seldon.minhash.numkeygroups");
		maxSharedInterests = Integer.parseInt(maxSharedInterestsStr);
		numKeyGroups = Integer.parseInt(numKeyGroupsStr);
		//optional start testing minhash
		TestMinHash.testMinHash(props);
	}
	
	public MinHashClusterPeer(MinHashSeedStore seedStore,StoredClusterHandler clusterStore)
	{
		this.clusterStore = clusterStore;
		this.seedStore = seedStore;
	}
	
	void createHashes(UserPeer u,ActionPeer a,int numHashes)
	{
		if (!clusterStore.hashesAvailable())
		{
			MinHashClustering.initialise(true,numHashes,seedStore);
			Collection<User> users = u.getActiveUsers(5000000);
			int i = 0;
			for(User user : users)
			{
				i++;
				logger.info("Storing hashes for user "+user.getUserId() + " number:"+i);
				Collection<Action> actions = a.getUserActions(user.getUserId(), 500000);
				Set<String> interests = new HashSet<String>();
				for(Action action : actions)
				{
					interests.add(""+action.getItemId());
				}
				MinHashClustering mh = new MinHashClustering(maxSharedInterests,numKeyGroups,clusterStore);
				
				//mh.setItemFreq(new ItemFrequencyPeer(a));
				mh.storeHashesForUser(user.getUserId(), interests);
			}
		}
		else {
//			logger.error("Hashes already exist - not creating new ones");
            final String message = "Hashes already exist - not creating new ones";
            logger.error(message, new Exception(message));
        }
	}
	
	
	
	
	public double[] test(long user,UserPeer uPeer,OpinionPeer oPeer,TrustNetworkSupplier tnetPeer,int numResults)
	{
		MinHashClustering mh = new MinHashClustering(maxSharedInterests,numKeyGroups,clusterStore);
		//mh.setItemFreq(new ItemFrequencyPeer(oPeer));
		Set<String> interests = new HashSet<String>();
		Collection<Opinion> opinions = oPeer.getUserOpinions(user, 5000000);
		for(Opinion opinion : opinions)
		{
			interests.add(""+opinion.getItemId());
		}
		logger.info("Testing user " + user + " with " + opinions.size() + " opinions");
		long start = System.currentTimeMillis();
		Map<Long,Integer> matches = mh.getSimilarUsers(user, interests, numResults);
		//Map<Long,Integer> matches = clusterStore.getUsersNonHash(interests,numResults);
		long end = System.currentTimeMillis();
		logger.info("minhash time " + (end-start));
		
		/*
		Set<Long> matches = new HashSet<Long>();
		Random r = new Random();
		while (matches.size()<numResults)
		{
			matches.add((long) r.nextInt(1269) + 1);
		}
		*/
		
		long numOp1 = oPeer.getNumOpinions(user);
		for(Long u : matches.keySet())
		{
			if (u != user)
			{
				long numSharedInterests = oPeer.getNumSharedOpinions(user, u);
				long numOp2 = oPeer.getNumOpinions(u);
				double jaccard = numSharedInterests / (double)(numOp1+numOp2-numSharedInterests);
				logger.info("similar user:"+u+" oc:"+numSharedInterests+" ja:"+jaccard);
			}
		}
		Collection<User> usersAll = uPeer.getActiveUsers(5000);
		Map<Long,Long> co = new HashMap<Long,Long>();
		Map<Long,Double> ja = new HashMap<Long,Double>();
		for(User user2 : usersAll)
		{
			if (user2.getUserId() != user)
			{
				long numSharedInterests = oPeer.getNumSharedOpinions(user, user2.getUserId());
				long numOp2 = oPeer.getNumOpinions(user2.getUserId());
				co.put(user2.getUserId(), numSharedInterests);
				ja.put(user2.getUserId(), numSharedInterests/(double)(numOp1+numOp2-numSharedInterests));
			}
		}
		
		RecommendationNetwork network = tnetPeer.getTrustNetwork(user, Trust.TYPE_GENERAL);
		List<Long> topN = network.getSimilarityNeighbourhood(numResults);
		double inTopN = 0;
		for(Long user2 : topN)
			if (matches.containsKey(user2))
				inTopN++;
		
		Map<Long,Long> topOc = CollectionTools.sortMapAndLimit(co, numResults);
		Map<Long,Double> topJa = CollectionTools.sortMapAndLimit(ja, numResults);
		logger.info("Top co-occurrences are:");
		int coCount = 0;
		int jaCount = 0;
		for(Map.Entry<Long, Long> e : topOc.entrySet())
		{
			if (matches.containsKey(e.getKey()))
				coCount++;
			logger.info("u:"+e.getKey()+" oc:"+e.getValue());
		}
		logger.info("Top jaccard are:");
		for(Map.Entry<Long, Double> e : topJa.entrySet())
		{
			if (matches.containsKey(e.getKey()))
				jaCount++;
			logger.info("u:"+e.getKey()+" ja:"+e.getValue());
		}		
		double coPercent = coCount/(double)topOc.size();
		double jaPercent = jaCount/(double)topJa.size();
		double topNPercent = inTopN/(double)numResults;
		return new double[] {coPercent,jaPercent,topNPercent,end-start};
		
	}
	
	
	private double createTrustFromSharedInterests(long numSharedInterests,double defaultPrior,double a)
	{
		double den = numSharedInterests+defaultPrior;
		double b = numSharedInterests/den;
		double u = 4/den;
		double E = b + u*a;
		return E;
	}
	
	
	public Map<Long,Double> findSimilarUsers(long user,Set<String> interests)
	{
		storeHashesForUser(user,interests);
		MinHashClustering mh = new MinHashClustering(maxSharedInterests,numKeyGroups,clusterStore);
		Map<Long,Integer> matches = mh.getSimilarUsers(user, interests, 20);
		Map<Long,Double> trustedUsers = new HashMap<Long,Double>();
		for(Map.Entry<Long, Integer> match : matches.entrySet())
		{
			if (match.getKey() != user)
			{
				//Note : hardwired values to create trust value from
				int positiveObservations = match.getValue()*5;
				double trust = createTrustFromSharedInterests(match.getValue()*5,2D,0.1D);
				trustedUsers.put(match.getKey(), trust);
			}
		}
		return trustedUsers;
	}
	
	public void createTrustNetwork(long user,Map<Long,Double> trustedUsers,Set<Long> friends,double friendsDefaultTrust,TrustNetworkSupplier tns)
	{
		//Store trusted users
		for(Map.Entry<Long, Double> trustedUser : trustedUsers.entrySet())
		{
			double trust = trustedUser.getValue();
			if (friends.contains(trustedUser.getKey()) && trust < friendsDefaultTrust)
				trust = friendsDefaultTrust;
				tns.addTrustLink(user, Trust.TYPE_GENERAL,trustedUser.getKey(), trust);
		}
		//store friends
		Set<Long> matchUsers = trustedUsers.keySet();
		for(Long friend : friends)
		{
			if (!matchUsers.contains(friend))
				tns.addTrustLink(user, Trust.TYPE_GENERAL,friend, friendsDefaultTrust);		
		}
	}
	
	public void storeHashesForUser(long user,Set<String> interests)
	{
		logger.info("Store hashes: maxSharedInterests " + maxSharedInterests + " numKeyGroups " + numKeyGroups);
		MinHashClustering mh = new MinHashClustering(maxSharedInterests,numKeyGroups,clusterStore);
		mh.storeHashesForUser(user, interests);
	}
	
}
