/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.api.state;

import io.seldon.plugins.PluginProvider;
import io.seldon.plugins.PluginService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class PluginConfigStore implements ApplicationContextAware,ClientConfigUpdateListener {
	protected static Logger logger = Logger.getLogger(PredictionAlgorithmStore.class.getName());
	private static final String PLUGIN_KEY = "plugins";
	
	private ConcurrentMap<String, List<PluginProvider>> pluginStore = new ConcurrentHashMap<>();
		 
	private final ClientConfigHandler configHandler;
	private ApplicationContext applicationContext;
	 
	 @Autowired
	 public PluginConfigStore(ClientConfigHandler configHandler)
	 {	
		 this.configHandler = configHandler;
	 }
	 
	 @PostConstruct
	 private void init(){
		 logger.info("Initializing...");
		 configHandler.addListener(this);
	    }

	 public List<PluginProvider> retrievePlugins(String client)
	 {
		 return pluginStore.get(client);
	 }
	 
	 private PluginProvider toPluginProvider(Plugin plugin) {
			PluginService service = applicationContext.getBean(plugin.name, PluginService.class);
			Map<String, String> config  = toConfigMap(plugin.config);
			return new PluginProvider(service,plugin.config ==null ? new HashMap<String, String>(): config , plugin.service);
		}
	 
	 
	 
	@Override
	public void configUpdated(String client, String configKey,String configValue) {
		if (configKey.equals(PLUGIN_KEY)){
			logger.info("Received new plugin config for "+ client+": "+ configValue);
			try {
				ObjectMapper mapper = new ObjectMapper();
				List<PluginProvider> plugins = new ArrayList<>();
				PluginConfig config = mapper.readValue(configValue, PluginConfig.class);
				for (Plugin plugin : config.plugins) {
					PluginProvider strategy = toPluginProvider(plugin);
					plugins.add(strategy);
				}
				pluginStore.put(client, plugins);
				logger.info("Successfully added new plugin config for "+client);
	            } catch (IOException | BeansException e) {
	                logger.error("Couldn't update plugin for client " +client, e);
	            }
		}
	}
	
	@Override
	public void configRemoved(String client, String configKey) {
		pluginStore.remove(client);
		logger.info("Removed client "+client);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		 this.applicationContext = applicationContext;
		 StringBuilder builder= new StringBuilder("Available algorithms: \n");
		 for (PluginService inc : applicationContext.getBeansOfType(PluginService.class).values()){
			 builder.append('\t');
			 builder.append(inc.getClass());
			 builder.append('\n');
		 }
		 logger.info(builder.toString());
	}
	
	private Map<String, String> toConfigMap(List<ConfigItem> config) {
		Map<String, String> configMap = new HashMap<>();
		if (config==null) return configMap;
		for (ConfigItem item : config){
			configMap.put(item.name,item.value);
		}
		return configMap;
	}
	
	 public static class PluginConfig {
	        public List<Plugin> plugins;
	    }

	    public static class Plugin {
	        public String name;
	        public String service;
	        public List<ConfigItem> config;
	    }

	    public static class ConfigItem {
	        public String name;
	        public String value;
	    }
	   

		
}
