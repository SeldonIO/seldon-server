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
 * *******************************************************************************************
 */

package io.seldon.api.statsd;

import io.seldon.api.state.GlobalConfigHandler;
import io.seldon.api.state.GlobalConfigUpdateListener;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StatsdPeer {
   
    public static class StatsdConfig {

        public String server;
        public int port;
        public String id = "sandbox";
        public float sample_rate = 1.0f;
        
        @Override
        public String toString() {
            return String.format(""
                    + "server[%s], "
                    + "port[%s], "
                    + "id[%s], "
                    + "sample_rate[%s]"
                    + "", server,port,id,sample_rate);
        }
    }
    
    @Component
    public static class ConfigListener implements GlobalConfigUpdateListener {

        private static Logger configListenerLogger = Logger.getLogger(ConfigListener.class.getName());
        private final String ZK_CONFIG_KEY_STATSD = "statsd";
        private final String ZK_CONFIG_KEY_STATSD_FPATH = "/config/" + ZK_CONFIG_KEY_STATSD;

        @Autowired
        public ConfigListener(GlobalConfigHandler globalConfigHandler) {
            configListenerLogger.info("Subscribing for updates to " + ZK_CONFIG_KEY_STATSD_FPATH);
            globalConfigHandler.addSubscriber(ZK_CONFIG_KEY_STATSD, this);
        }

        @Override
        public void configUpdated(String configKey, String configValue) {
            configListenerLogger.info(String.format("received config update %s[%s]", configKey, configValue));
            if ((StatsdPeer.client == null) && (configValue.length() > 0)) {
                ObjectMapper mapper = new ObjectMapper();
                try {
                    StatsdConfig statsdConfig = mapper.readValue(configValue, StatsdConfig.class);
                    StatsdPeer.initialise(statsdConfig);
                } catch (Exception e) {
                    throw new RuntimeException(String.format("* Error * parsing statsd configValue[%s]", configValue),e);
                }
            }
        }
    }

	private static Logger logger = Logger.getLogger(StatsdPeer.class.getName());
	
	private static StatsdClient client;
	static String installId = "sandbox";
	static float sampleRate = 1.0f;

	public static boolean initialise(StatsdConfig statsdConfig) {

		String statsdServer = statsdConfig.server;
		int port = statsdConfig.port;
		if (statsdServer != null && port != 0)
		{
			try {
				logger.info("Creating statsd client for host "+statsdServer+" on port "+port+" and using install id of "+installId+" and default sampling of "+sampleRate);
				installId = statsdConfig.id;
				sampleRate = statsdConfig.sample_rate;
				client = new StatsdClient(statsdServer, port);
				return true;
			} catch (UnknownHostException e) {
				logger.error("Failed to create statsdClient ",e);
				return false;
			} catch (IOException e) {
				logger.error("Failed to create statsdClient ",e);
				return false;
			}
		}
		else
		{
			logger.info("No statsd properties not creating client");
			return false;
		}
	}
	
	@Deprecated
	public static boolean initialise(Properties props) 
	{
		String statsdServer = props.getProperty("statsd.server");
		String portStr = props.getProperty("statsd.port");
		if (props.containsKey("statsd.id"))
			installId = props.getProperty("statsd.id");
		if (props.containsKey("statsd.sample.rate"))
			sampleRate = Float.parseFloat(props.getProperty("statsd.sample.rate"));
		if (statsdServer != null && portStr != null)
		{
			int port = Integer.parseInt(portStr);
			try {
				logger.info("Creating statsd client for host "+statsdServer+" on port "+port+" and using install id of "+installId+" and default sampling of "+sampleRate);
				client = new StatsdClient(statsdServer, port);
				return true;
			} catch (UnknownHostException e) {
				logger.error("Failed to create statsdClient ",e);
				return false;
			} catch (IOException e) {
				logger.error("Failed to create statsdClient ",e);
				return false;
			}
		}
		else
		{
			logger.info("No statsd properties not creating client");
			return false;
		}
	}
	
	public static void logCTR(String consumerName,String algKey,boolean success,String abTestingKey,String recTag)
	{
		if (client != null)
		{
			try
			{
				client.increment(StatsdKeys.getClick(consumerName, algKey));
				if (abTestingKey != null)
				{
					if (recTag == null)
						client.increment(StatsdKeys.getClickABTesting(consumerName, algKey,abTestingKey));
					else
						client.increment(StatsdKeys.getClickABTesting(consumerName, algKey,abTestingKey,recTag));
				}
				if (success) {
					client.increment(StatsdKeys.getPositiveClick(consumerName, algKey));
					if (abTestingKey != null)
					{
						if (recTag == null)
							client.increment(StatsdKeys.getPositiveClickABTesting(consumerName, algKey,abTestingKey));
						else
							client.increment(StatsdKeys.getPositiveClickABTesting(consumerName, algKey,abTestingKey,recTag));
					}
				}
			}
			catch (Exception e)
			{
				logger.error("Failed to call statsd to log for consumer "+consumerName+" alg "+algKey);
			}
		}
	}
	
	//Log when a click has been generated from the RL recommendations
	public static void logClick(String consumerName,String recTag)
	{
		if (client != null)
		{
			try
			{
				if (recTag != null)
					client.increment(StatsdKeys.getClick(consumerName,recTag));
				else
					client.increment(StatsdKeys.getClick(consumerName));
			}
			catch (Exception e)
			{
				logger.error("Failed to call statsd to log for consumer "+consumerName);
			}
		}
	}
	
	//Log when a recommendations set has been shown/retrieved
	public static void logImpression(String consumerName,String recTag)
	{
		if (client != null)
		{
			try
			{
				if (recTag != null)
					client.increment(StatsdKeys.getImpression(consumerName,recTag));
				else
					client.increment(StatsdKeys.getImpression(consumerName));
			}
			catch (Exception e)
			{
				logger.error("Failed to call statsd to log for consumer "+consumerName);
			}
		}
	}
	
	
	public static void logAPICall(String consumerName,String apiKey,String httpMethod,int time)
	{
		if (client != null)
		{
			try
			{
				String baseKey = StatsdKeys.getAPIKey(consumerName, apiKey, httpMethod);
				client.increment(baseKey+".count",1,sampleRate);
				client.timing(baseKey+".time", time, sampleRate);
			}
			catch (Exception e)
			{
				logger.error("Failed to call statsd to log for consumer "+consumerName+" apikey "+apiKey+" httpMethod "+httpMethod+" time "+time,e);
			}
		}
	}


}
