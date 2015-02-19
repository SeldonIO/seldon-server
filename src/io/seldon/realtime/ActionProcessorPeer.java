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

package io.seldon.realtime;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import io.seldon.realtime.kafka.KafkaActionProcessor;
import io.seldon.realtime.kafka.KafkaSetupException;
import io.seldon.realtime.kestrel.KestrelActionProcessor;

public class ActionProcessorPeer {
private static Logger logger = Logger.getLogger(ActionProcessorPeer.class.getName());
	
	static ConcurrentHashMap<String,IActionProcessor> store = new ConcurrentHashMap<>();
	
	private static final int DEF_KAFKA_PORT = 9092;
	private static final String DEF_TYPE = "kafka";
	
	
	public static void initialise(Properties props) throws KafkaSetupException
	{
		String clientsStr = props.getProperty("io.seldon.realtime.clients");
		if(clientsStr != null && clientsStr.length() > 0)
		{
			String clients[] = clientsStr.split(",");
			for(int i=0;i<clients.length;i++)
			{
				String client = clients[i];
				String type = props.getProperty("io.seldon.realtime."+client+".type");
				if (type == null)
					type = DEF_TYPE;
				if ("kestrel".equals(type))
				{
					String qname = props.getProperty("io.seldon.realtime."+client+".kestrel.qname");
					String host = props.getProperty("io.seldon.realtime."+client+".kestrel.host");
					String portStr = props.getProperty("io.seldon.realtime."+client+".kestrel.port");
					logger.info("Creating realtime kestrel q for client "+client+" with q "+qname+" host "+host+" port "+portStr);
					int port = Integer.parseInt(portStr);
					try
					{
						store.put(client, new KestrelActionProcessor(qname,host,port));
					} catch (IOException e) {
						logger.error("Failed to create Kestrel client for client "+client,e);
					}
				}
				else if ("kafka".equals(type))
				{
					String topic = props.getProperty("io.seldon.realtime."+client+".kafka.topic");
					if (topic == null)
						topic = client;
					String host = props.getProperty("io.seldon.realtime."+client+".kafka.host");
					if (host == null)
						host = props.getProperty("io.seldon.realtime.kafka.host");
					if (host == null)
					{
						logger.error("Can't find property io.seldon.realtime.kafka.host and there is none set for client "+client);
						throw new KafkaSetupException();
					}
					String portStr = props.getProperty("io.seldon.realtime."+client+".kafka.port");
					int port = DEF_KAFKA_PORT;
					if (portStr != null)
						port = Integer.parseInt(portStr);
					logger.info("Creating realtime kafka q for client "+client+" with topic "+topic+" host "+host+" port "+port);
					store.put(client, new KafkaActionProcessor(host,port,topic));
				}
					
			}
		}
	}
	
	public static IActionProcessor getActionProcessor(String client)
	{
		return store.get(client);
	}
}
