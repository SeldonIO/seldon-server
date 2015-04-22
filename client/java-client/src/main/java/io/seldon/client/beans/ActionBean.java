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

import java.util.Date;
import java.util.List;
import org.springframework.stereotype.Component;


@Component
public class ActionBean extends ResourceBean {

    private static final long serialVersionUID = -4041317676541200339L;
    
    private Long actionId;
	private String user;
	private String item;
	private int type;
	private Date date;
	private Double value;
	private Integer times = 1; // workaround (null isn't accepted by the server)
	private String comment;
	private List<String> tags;
	private String recTag;
	private String referrer;
	
	public ActionBean() {}

    public ActionBean(String user, String item, int type, Date date, Double value) {
        this.user = user;
        this.item = item;
        this.type = type;
        this.date = date;
        this.value = value;
    }

    public ActionBean(String user, String item, int type) {
        this.user = user;
        this.item = item;
        this.type = type;
    }

    public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public Integer getTimes() {
		return times;
	}

	public void setTimes(Integer times) {
		this.times = times;
	}

	public Long getActionId() {
		return actionId;
	}

	public void setActionId(Long actionId) {
		this.actionId = actionId;
	}
		
	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getComment() { return this.comment; }
	
	public void setComment(String c) { this.comment=c; }
	
    public String getRecTag() {
        return recTag;
    }

    public void setRecTag(String recTag) {
        this.recTag = recTag;
    }

	public String getReferrer() {
		return referrer;
	}

	public void setReferrer(String referrer) {
		this.referrer = referrer;
	}

	
    @Override
    public String toString() {
        return "ActionBean{" +
                "actionId=" + actionId +
                ", user='" + user + '\'' +
                ", item='" + item + '\'' +
                ", type=" + type +
                ", date=" + date +
                ", value=" + value +
                ", times=" + times +
                ", comment='" + comment + '\'' +
                ", tags=" + tags +
                ", referrer=" + referrer +
                ", recTag=" + recTag +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ActionBean)) return false;

        ActionBean that = (ActionBean) o;

        if (type != that.type) return false;
        if (actionId != null ? !actionId.equals(that.actionId) : that.actionId != null) return false;
        if (comment != null ? !comment.equals(that.comment) : that.comment != null) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        if (item != null ? !item.equals(that.item) : that.item != null) return false;
        if (tags != null ? !tags.equals(that.tags) : that.tags != null) return false;
        if (times != null ? !times.equals(that.times) : that.times != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = actionId != null ? actionId.hashCode() : 0;
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (item != null ? item.hashCode() : 0);
        result = 31 * result + type;
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (times != null ? times.hashCode() : 0);
        result = 31 * result + (comment != null ? comment.hashCode() : 0);
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }
}

