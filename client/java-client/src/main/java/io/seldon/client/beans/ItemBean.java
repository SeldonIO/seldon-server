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
package io.seldon.client.beans;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class ItemBean extends ResourceBean {

    private static final long serialVersionUID = -3283295256302891263L;
    
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

    public ItemBean(String id, String name, int type) {
        this.id = id;
        this.name = name;
        this.type = type;
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

	@Override
    public String toString() {
        return "ItemBean{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", first_action=" + first_action +
                ", last_action=" + last_action +
                ", popular=" + popular +
                ", attributes=" + attributes +
                ", attributesName=" + attributesName +
                ", demographics=" + demographics +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemBean)) return false;

        ItemBean itemBean = (ItemBean) o;

        if (popular != itemBean.popular) return false;
        if (type != itemBean.type) return false;
        if (attributes != null ? !attributes.equals(itemBean.attributes) : itemBean.attributes != null) return false;
        if (attributesName != null ? !attributesName.equals(itemBean.attributesName) : itemBean.attributesName != null)
            return false;
        if (demographics != null ? !demographics.equals(itemBean.demographics) : itemBean.demographics != null)
            return false;
        if (first_action != null ? !first_action.equals(itemBean.first_action) : itemBean.first_action != null)
            return false;
        if (id != null ? !id.equals(itemBean.id) : itemBean.id != null) return false;
        if (last_action != null ? !last_action.equals(itemBean.last_action) : itemBean.last_action != null)
            return false;
        if (name != null ? !name.equals(itemBean.name) : itemBean.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + type;
        result = 31 * result + (first_action != null ? first_action.hashCode() : 0);
        result = 31 * result + (last_action != null ? last_action.hashCode() : 0);
        result = 31 * result + (popular ? 1 : 0);
        result = 31 * result + (demographics != null ? demographics.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (attributesName != null ? attributesName.hashCode() : 0);
        return result;
    }
}
