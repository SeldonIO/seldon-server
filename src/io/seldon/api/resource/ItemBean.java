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

package io.seldon.api.resource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import io.seldon.api.APIException;
import io.seldon.api.Util;
import io.seldon.api.resource.service.ItemService;
import io.seldon.api.resource.service.UserService;
import io.seldon.general.ItemDemographic;
import org.springframework.stereotype.Component;
import io.seldon.general.Item;


/**
 * @author claudio
 */

@Component
public class ItemBean extends ResourceBean {
	
	String id;
	String name;
	int type;
	Date first_action;
	Date last_action;
	boolean popular;
	ArrayList<DemographicBean> demographics;
	Map<Integer,Integer> attributes;
	Map<String,String> attributesName;
	
	public ItemBean() {
	}

	public ItemBean(String id) {
		this.id = id;
	}
	
	public ItemBean(ConsumerBean c,long internalItemId) {
		this.id = ItemService.getClientItemId(c, internalItemId);
	}
	
	public ItemBean(String id,String name) {
		this.id = id;
		this.name = name;
	}
	
	public ItemBean(String id,String name, int type) {
		this.id = id;
		this.name = name;
		this.type = type;
	}
	
	public ItemBean(Item i)
	{
		this(i,false,null);
	}
	
	public ItemBean(Item i,boolean full, ConsumerBean c) throws APIException {
		id = i.getClientItemId();
		name = i.getName();
		type = i.getType();
		first_action = i.getFirstOp();
		last_action = i.getLastOp();
		popular = i.isPopular();
		if(full) {
			attributes = Util.getItemPeer(c).getItemAttributes(i.getItemId());
			attributesName = Util.getItemPeer(c).getItemAttributesName(i.getItemId());
			//Demographic
			demographics = new ArrayList<DemographicBean>();
			Collection<ItemDemographic> demos = Util.getItemPeer(c).getItemDemographics(i.getItemId());
			for(ItemDemographic id : demos) {
				DemographicBean d = UserService.getDemographic(c, id.getDemoId());
				d.setAmount(id.getRelevance());
				demographics.add(d);
			}
		}
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<Integer, Integer> getAttributes() {
		return attributes;
	}

	public void setAttributes(Map<Integer, Integer> attributes) {
		this.attributes = attributes;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public Date getFirst_action() {
		return first_action;
	}

	public void setFirst_action(Date firstAction) {
		first_action = firstAction;
	}

	public Date getLast_action() {
		return last_action;
	}

	public void setLast_action(Date lastAction) {
		last_action = lastAction;
	}

	public boolean isPopular() {
		return popular;
	}

	public void setPopular(boolean popular) {
		this.popular = popular;
	}

	public Map<String, String> getAttributesName() {
		return attributesName;
	}

	public void setAttributesName(Map<String, String> attributesName) {
		this.attributesName = attributesName;
	}
	
	
	public ArrayList<DemographicBean> getDemographics() {
		return demographics;
	}

	public void setDemographics(ArrayList<DemographicBean> demographics) {
		this.demographics = demographics;
	}
	
	public Item createItem(ConsumerBean c) {
		Item i = new Item();
		i.setClientItemId(this.id);
		i.setName(this.name);
		i.setFirstOp(this.first_action);
		i.setLastOp(this.last_action);
		i.setType(this.type);
		i.setPopular(this.popular);
		return i;
	}


	@Override
	public String toKey() {
		return id;
	}
	
	@Override
	public String toLog() {
		return id+";"+type;
	}
}
