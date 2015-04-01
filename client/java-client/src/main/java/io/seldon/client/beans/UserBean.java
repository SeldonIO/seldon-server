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
public class UserBean extends ResourceBean {

    private static final long serialVersionUID = 1049395595605751349L;
    
    private String id;
	private String username;
	private Date first_action;
	private Date last_action;
	private int type;
	private int num_actions;
	private boolean active;
	private ArrayList<DimensionBean> dimensions;
	private Map<Integer,Integer> attributes;
	private Map<String,String> attributesName;
	
	public UserBean() {
	}

    public UserBean(String id, String username) {
        this.id = id;
        this.username = username;
    }

    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
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

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getNum_actions() {
		return num_actions;
	}

	public void setNum_actions(int numActions) {
		num_actions = numActions;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public ArrayList<DimensionBean> getDimensions() {
		return dimensions;
	}

	public void setDimensions(ArrayList<DimensionBean> dimensions) {
		this.dimensions = dimensions;
	}


    public Map<Integer, Integer> getAttributes() {
		return attributes;
	}


	public void setAttributes(Map<Integer, Integer> attributes) {
		this.attributes = attributes;
	}


	public Map<String, String> getAttributesName() {
		return attributesName;
	}


	public void setAttributesName(Map<String, String> attributesName) {
		this.attributesName = attributesName;
	}

    @Override
    public String toString() {
        return "UserBean{" +
                "id='" + id + '\'' +
                ", username='" + username + '\'' +
                ", first_action=" + first_action +
                ", last_action=" + last_action +
                ", type=" + type +
                ", num_actions=" + num_actions +
                ", active=" + active +
                ", dimensions=" + dimensions +
                ", attributes=" + attributes +
                ", attributesName=" + attributesName +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserBean)) return false;

        UserBean userBean = (UserBean) o;

        if (active != userBean.active) return false;
        if (num_actions != userBean.num_actions) return false;
        if (type != userBean.type) return false;
        if (attributes != null ? !attributes.equals(userBean.attributes) : userBean.attributes != null) return false;
        if (attributesName != null ? !attributesName.equals(userBean.attributesName) : userBean.attributesName != null)
            return false;
        if (dimensions != null ? !dimensions.equals(userBean.dimensions) : userBean.dimensions != null) return false;
        if (first_action != null ? !first_action.equals(userBean.first_action) : userBean.first_action != null)
            return false;
        if (id != null ? !id.equals(userBean.id) : userBean.id != null) return false;
        if (last_action != null ? !last_action.equals(userBean.last_action) : userBean.last_action != null)
            return false;
        if (username != null ? !username.equals(userBean.username) : userBean.username != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (first_action != null ? first_action.hashCode() : 0);
        result = 31 * result + (last_action != null ? last_action.hashCode() : 0);
        result = 31 * result + type;
        result = 31 * result + num_actions;
        result = 31 * result + (active ? 1 : 0);
        result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (attributesName != null ? attributesName.hashCode() : 0);
        return result;
    }
}
