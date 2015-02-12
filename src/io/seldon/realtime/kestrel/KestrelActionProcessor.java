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

package io.seldon.realtime.kestrel;

import java.io.IOException;

import io.seldon.realtime.IActionProcessor;
import org.apache.log4j.Logger;

import backtype.storm.spout.KestrelClient;
import backtype.storm.spout.KestrelClient.ParseError;

public class KestrelActionProcessor implements IActionProcessor {

	private static Logger logger = Logger.getLogger(KestrelActionProcessor.class.getName());
	
	KestrelClient client;
	String host;
	int port;
	String qName;
	
	public KestrelActionProcessor(String qName,String host,int port) throws IOException
	{
		this.host = host;
		this.port = port;
		this.qName = qName;
		client = new KestrelClient(host,port);
	}
	
	@Override
	public void addAction(long userId, long itemId, double value, long time,int type)  {
		if (client != null)
		{
			boolean ok = false;
			final int numRetries = 3;
			for(int i=0;i<numRetries && !ok && client != null;i++)
			{
				try
				{
					client.queue(qName, ""+userId+","+itemId+","+value+","+time+","+type);
					ok = true;
				}
				catch (IOException e)
				{
					try 
					{
						logger.error("Error on queueing action",e);
						client = new KestrelClient(host,port);
					} catch (IOException e1) 
					{
						logger.error("Failed to recreate kestrel client ",e1);
						client = null;
					}
				} catch (ParseError e) {
					logger.error("Failed to queue action ",e);
					ok = true; //don't try again
				}
			}
			if (!ok)
			{
				logger.error("Failing and setting client to null. No more messages will be queued");
				client = null;
			}
		}
	}

}
