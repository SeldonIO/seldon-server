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
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class OpinionBean extends ResourceBean {
    private static final long serialVersionUID = -6272567701517314784L;

    String item;
	String user;
	Double value;
	boolean prediction;
	Date time;
	List<Long> srcUsers;
	
	public OpinionBean() {};

	public String getItem() {
		return item;
	}

	public void setItem(String item) {
		this.item = item;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	public boolean getPrediction() {
		return prediction;
	}

	public void setPrediction(boolean prediction) {
		this.prediction = prediction;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}


	public List<Long> getSrcUsers() {
		return srcUsers;
	}


	public void setSrcUsers(List<Long> srcUsers) {
		this.srcUsers = srcUsers;
	}

	public void addSrcUser(Long u) {
		srcUsers.add(u);
	}

	public void initSrcUsers() {
		srcUsers = new ArrayList<Long>();
		
	}

    @Override
    public String toString() {
        return "OpinionBean{" +
                "item='" + item + '\'' +
                ", user='" + user + '\'' +
                ", value=" + value +
                ", prediction=" + prediction +
                ", time=" + time +
                ", srcUsers=" + srcUsers +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OpinionBean)) return false;

        OpinionBean that = (OpinionBean) o;

        if (prediction != that.prediction) return false;
        if (item != null ? !item.equals(that.item) : that.item != null) return false;
        if (srcUsers != null ? !srcUsers.equals(that.srcUsers) : that.srcUsers != null) return false;
        if (time != null ? !time.equals(that.time) : that.time != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = item != null ? item.hashCode() : 0;
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (prediction ? 1 : 0);
        result = 31 * result + (time != null ? time.hashCode() : 0);
        result = 31 * result + (srcUsers != null ? srcUsers.hashCode() : 0);
        return result;
    }
}