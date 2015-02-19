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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.jdo.PersistenceManager;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.clustering.minhash.jdo.MinHashSeedStorePeer;
import io.seldon.clustering.minhash.jdo.StoredClusterPeer;
import io.seldon.db.jdo.JDOFactory;
import io.seldon.general.UserPeer;
import io.seldon.trust.impl.CFAlgorithm;
import io.seldon.trust.impl.jdo.SimpleTrustNetworkProvider;
import org.apache.log4j.Logger;

import io.seldon.general.User;

public class TestMinHash extends Thread {
	private static Logger logger = Logger.getLogger(TestMinHash.class.getName());
	String facebookDb;
	
	int numUsers = 5000;
	int numResults = 10;
	
	public TestMinHash(String db,int numResults)
	{
		this.numResults = numResults;
		this.facebookDb = db;
	}
	
	public static void testMinHash(Properties props)
	{
		String createHashes = props.getProperty("io.seldon.minhash.test.createHashes");
		String facebookDb = props.getProperty("io.seldon.minhash.test.db");
		String numHashesStr = props.getProperty("io.seldon.minhash.numhashes");
		int numHashes = Integer.parseInt(numHashesStr);
		
		logger.info("Create Hashes=["+createHashes+"] facebook db ["+facebookDb+"] numHashes ["+numHashes+"]");
		if (facebookDb != null && createHashes != null && "true".equals(createHashes))
		{
			PersistenceManager pm = JDOFactory.getPersistenceManager(facebookDb);
			if (pm != null)
			{
				logger.info("Creating min hashes");
				MinHashClusterPeer mhp = new MinHashClusterPeer(new MinHashSeedStorePeer(pm),new StoredClusterPeer(pm));
				mhp.createHashes(Util.getUserPeer(pm), Util.getActionPeer(pm), numHashes);
			}
			else {
//				logger.error("Can't get pm to create min hashes");
                final String message = "Can't get pm to create min hashes";
                logger.error(message, new Exception(message));
            }
		}
		else
		{
			PersistenceManager pm = JDOFactory.getPersistenceManager(facebookDb);
			if (pm != null)
			{
				MinHashClustering.initialise(false, numHashes, new MinHashSeedStorePeer(pm));
			}
			String testHashes = props.getProperty("io.seldon.minhash.test");
			if (testHashes != null && "true".equals(testHashes))
			{
				int numTestResultsPerUser = 10;
				TestMinHash testMinHash = new TestMinHash(facebookDb,numTestResultsPerUser);
				testMinHash.start();
			}
		}
	}
	
	/**
	 * Test made more complex as DN JDO seems to get slower and slower if we keep the same persistenceManager 
	 */
	public void run()
	{
		
		PersistenceManager pm = JDOFactory.getPersistenceManager(facebookDb);
		UserPeer uPeer = Util.getUserPeer(pm);
		Collection<User> users = uPeer.getActiveUsers(numUsers);
		Set<Long> uIds = new HashSet<>(numUsers);
		for(User u : users)
			uIds.add(u.getUserId());
		JDOFactory.cleanupPM();
		
		double count = 0;
		double coSum = 0;
		double jaSum = 0;
		double topNSum = 0;
		double timeSum = 0;
		logger.info("Will test "+uIds.size()+" users");
		for(Long u : uIds) {
            // get new pm for this user
            pm = JDOFactory.getPersistenceManager(facebookDb);

            // do test for this user
            count++;
            logger.info("Test number:" + count);
            logger.info("Testing user " + u);
            CFAlgorithm cfAlgorithm;
            try {
                cfAlgorithm = Util.getAlgorithmService().getAlgorithmOptions(facebookDb);
            } catch (CloneNotSupportedException e) {
                throw new APIException(APIException.CANNOT_CLONE_CFALGORITHM);
            }
            MinHashClusterPeer mhp = new MinHashClusterPeer(new MinHashSeedStorePeer(pm), new StoredClusterPeer(pm));
            double[] res = mhp.test(u, Util.getUserPeer(pm), Util.getOpinionPeer(pm), new SimpleTrustNetworkProvider(cfAlgorithm), numResults);
            double coPercent = res[0];
            double jaPercent = res[1];
            double topN = res[2];
            double time = res[3];
            timeSum = timeSum + time;
            logger.info("Co% " + coPercent + " ja% " + jaPercent + " topN:" + topN);
            coSum = coSum + coPercent;
            jaSum = jaSum + jaPercent;
            topNSum = topNSum + topN;
            double avgCoPercent = coSum / count;
            double avgJaPercent = jaSum / count;
            double avgTopN = topNSum / count;
            double timeAvg = timeSum / count;
            logger.info("Running CoAvg% " + avgCoPercent + " jaAvg% " + avgJaPercent + " topN%:" + avgTopN + " avgTime:" + timeAvg);

            // cleanup JDO after this user
            JDOFactory.cleanupPM();
        }
		double avgCoPercent = coSum / count;
		double avgJaPercent = jaSum / count;
		double avgTopN = topNSum / count;
		double timeAvg = timeSum / count;
		logger.info("CoAvg% " + avgCoPercent + " jaAvg% " + avgJaPercent + " topN%:"+avgTopN+" avgTime:"+timeAvg);
	}
}
