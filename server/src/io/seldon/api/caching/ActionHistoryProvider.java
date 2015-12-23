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
package io.seldon.api.caching;

import io.seldon.api.APIException;
import io.seldon.api.state.ClientConfigHandler;
import io.seldon.api.state.ClientConfigUpdateListener;
import io.seldon.general.Action;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class ActionHistoryProvider implements ApplicationContextAware,ClientConfigUpdateListener,ActionHistory {
	private static Logger logger = Logger.getLogger(ActionHistoryProvider.class.getName());
	
	Map<String,ActionProvider> providers = new ConcurrentHashMap<String,ActionProvider>();
	@Autowired
	MemcacheActionHistory defaultProvider;
	public static final String ACTION_HISTORY_KEY = "action_history";
	private ApplicationContext applicationContext;
	
	@Autowired
	public ActionHistoryProvider(ClientConfigHandler configHandler)
	{
		configHandler.addListener(this);
	}

	@Override
	public void configUpdated(String client, String configKey,
			String configValue) {
		if (configKey.equals(ACTION_HISTORY_KEY)){
			logger.info("Received new action provider config for "+ client+": "+ configValue);
			try {
				ObjectMapper mapper = new ObjectMapper();
				ActionHistoryConfig config = mapper.readValue(configValue, ActionHistoryConfig.class);
				ActionHistory provider = applicationContext.getBean(config.type,ActionHistory.class);
				ActionProvider ap = new ActionProvider(provider, config.addActions);
				providers.put(client, ap);
				logger.info("Successfully added new action provider config for "+client);
	            } catch (IOException | BeansException e) {
	                logger.error("Couldn't update action provider for client " +client, e);
	            }
		}
	}

	@Override
	public void configRemoved(String client, String configKey) {
		if (configKey.equals(ACTION_HISTORY_KEY)){
			logger.info("Removing action provider for "+client);
			providers.remove(client);
		}
	}
	
	public static class ActionHistoryConfig {
		public String type;
		public boolean addActions = true;
	}
	
	public static class ActionProvider {
		final public ActionHistory provider;
		final public boolean addActions;
		public ActionProvider(ActionHistory provider, boolean addActions) {
			super();
			this.provider = provider;
			this.addActions = addActions;
		}
		
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
        this.applicationContext = applicationContext;
        StringBuilder builder= new StringBuilder("Available action history providers: \n");
        for (ActionHistory ah : applicationContext.getBeansOfType(ActionHistory.class).values()){
            builder.append('\t');
            builder.append(ah.getClass());
            builder.append('\n');
        }
        logger.info(builder.toString());
	}

	@Override
	public List<Long> getRecentActions(String clientName, long userId,
			int numActions) {
		ActionProvider ap = providers.get(clientName);
		if (ap != null)
			return ap.provider.getRecentActions(clientName, userId, numActions);
		else
			return defaultProvider.getRecentActions(clientName, userId, numActions);
	}

	@Override
	public List<Action> getRecentFullActions(String clientName, long userId,
			int numActions) {
		ActionProvider ap = providers.get(clientName);
		if (ap != null)
			return ap.provider.getRecentFullActions(clientName, userId, numActions);
		else
			return defaultProvider.getRecentFullActions(clientName, userId, numActions);
	}

	@Override
	public void addFullAction(String clientName, Action a) throws APIException {
		ActionProvider ap = providers.get(clientName);
		if (ap != null)
		{
			if (ap.addActions)
				ap.provider.addFullAction(clientName, a);
		}
		else
			defaultProvider.addFullAction(clientName, a);
	}

	@Override
	public void addAction(String clientName, long userId, long itemId)
			throws APIException {
		ActionProvider ap = providers.get(clientName);
		if (ap != null)
		{
			if (ap.addActions)
				ap.provider.addAction(clientName, userId, itemId);
		}
		else
			defaultProvider.addAction(clientName, userId, itemId);
	}
	
}
