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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

public class LeadershipContainer {

	private static final String DEF_HOSTNAME = "TEST";
	private static Logger logger = Logger.getLogger(LeadershipContainer.class.getName());

	private static LeadershipContainer theContainer;
	
	public static void initialise(Properties props)
	{
		//UNFINISHED LEADERSHIP HANDLER
	}
	
	private static String getHostname()
	{
		try
		{
			String EC2Id = "";
			String inputLine;
			URL EC2MetaData = new URL("http://169.254.169.254/latest/meta-data/local-hostname");
			HttpURLConnection EC2MD = (HttpURLConnection) EC2MetaData.openConnection();
			HttpURLConnection.setFollowRedirects(false);
			EC2MD.setConnectTimeout(5 * 1000);
			BufferedReader in = new BufferedReader(new InputStreamReader(EC2MD.getInputStream()));
			while ((inputLine = in.readLine()) != null)
			{
				EC2Id = inputLine;
			}
			in.close();
			return EC2Id;
		}
		catch (IOException e)
		{
			logger.warn("Failed to get EC2 local hostname",e);
			return DEF_HOSTNAME;
		}
	}
	
	public static void main(String[] args)
	{
		System.out.println("Hostname is "+getHostname());
	}
	
	LeadershipPeer leadershipPeer;
	Thread leadershipThread;
	private LeadershipContainer(String zkServer,String service)
	{
		leadershipPeer = new LeadershipPeer(zkServer, service);
		leadershipThread = new Thread(leadershipPeer);
		leadershipThread.start();
	}

	public static boolean isLeader()
	{
		if (theContainer != null)
			return theContainer.leadershipPeer.isLeader();
		else
		{
			logger.warn("No leadership container so returning false for isLeader");
			return false;
		}
	}
}
