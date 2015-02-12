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

package io.seldon.api.state;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.datanucleus.util.StringUtils;

public class ZkAlgorithmUpdaterFactory {

	private static Logger logger = Logger.getLogger(ZkAlgorithmUpdaterFactory.class.getName());
	
	private final static String UPDATERS = "io.seldon.updater.clients";

    private static Set<ZkAlgorithmUpdater> algorithmUpdaters = new HashSet<ZkAlgorithmUpdater>();
    private static Set<ZkABTestingUpdater> abtestUpdaters = new HashSet<ZkABTestingUpdater>();
	private static ThreadGroup group = new ThreadGroup("ZkUpdaters");


	public static void initialise(Properties props,ZkCuratorHandler curatorHandler)
	{
		String algClients = props.getProperty(UPDATERS);
		if (!StringUtils.isEmpty(algClients))
		{
			String[] clients = algClients.split(",");
			for(int i=0;i<clients.length;i++)
			{
				logger.info("Starting algorithm updater for client "+clients[i]);
				ZkAlgorithmUpdater updater = new ZkAlgorithmUpdater(clients[i], curatorHandler);
				Thread t = new Thread(group,updater);
				t.setName("AlgorithmUpdater_"+clients[i]);
                t.setDaemon(true);
                t.start();

                algorithmUpdaters.add(updater);
				logger.info("Starting AB Testing updater for client "+clients[i]);
				ZkABTestingUpdater updaterAB = new ZkABTestingUpdater(clients[i], curatorHandler);
				t = new Thread(group,updaterAB);
				t.setName("ABTestingUpdater_"+clients[i]);
                t.setDaemon(true);
				t.start();
                abtestUpdaters.add(updaterAB);

			}
		}
		
	}

    public static void destroy() throws InterruptedException {
        for(ZkAlgorithmUpdater t: algorithmUpdaters){
            t.setKeepRunning(false);
        }

        for(ZkABTestingUpdater t: abtestUpdaters){
            t.setKeepRunning(false);
        }

        group.interrupt();
        while(group.activeCount()!=0){
            Thread.sleep(100);
        }
        group.destroy();
    }
	
}
