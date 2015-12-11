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
package io.seldon.api.resource.service.business;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ItemBean;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.state.PluginConfigStore;
import io.seldon.api.state.options.DefaultOptions;
import io.seldon.clustering.recommender.RecommendationContext.OptionsHolder;
import io.seldon.plugins.PluginProvider;

import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PluginBusinessServiceImpl implements PluginBusinessService {
	private static Logger logger = Logger.getLogger(PluginBusinessServiceImpl.class.getName());
	
	public static final String PLUGIN_TELEPATH_NAME = "telepath";
	
	@Autowired
	private ItemService itemService;
	
	@Autowired
	PluginConfigStore pluginConfig;
	
	DefaultOptions defaultOptions;
	
	private PluginProvider findPlugin(String client,String name)
	{
		List<PluginProvider> plugins = pluginConfig.retrievePlugins(client);
		if (plugins != null)
		{
			for (PluginProvider pluginProvider : plugins)
			{
				if (pluginProvider.name.equals(name))
					return pluginProvider;
			}
			logger.warn("Failed to find plugin "+name+" for "+client);
			return null;
		}
		else
		{
			logger.warn("Failed to find plugin "+name+" for "+client);
			return null;
		}
	}
	
	@Override
	public JsonNode telepath_tag_prediction(final ConsumerBean consumerBean,final String client_item_id,final String attrName) 
	{
		
		PluginProvider telepathProvider = findPlugin(consumerBean.getShort_name(), PLUGIN_TELEPATH_NAME);
		if (telepathProvider != null)
		{
			// get item and text
			ItemBean item = itemService.getItem(consumerBean, client_item_id, true);
			String text = item.getAttributesName().get(attrName);
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode dataTable = mapper.createObjectNode();
			dataTable.put("text", text);
			OptionsHolder optsHolder = new OptionsHolder(defaultOptions, telepathProvider.config);
			return telepathProvider.pluginService.execute(dataTable,optsHolder);
		}
		else
			throw new APIException(APIException.PLUGIN_NOT_ENABLED);
	}

}
