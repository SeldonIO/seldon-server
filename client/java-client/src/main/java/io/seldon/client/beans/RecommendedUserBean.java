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

import java.util.List;

import org.springframework.stereotype.Component;

/**
 * @author claudio
 */

@Component
public class RecommendedUserBean extends ResourceBean {
    private static final long serialVersionUID = 2576292153652510258L;

    private String user;
	private Double score;
	private List<String> items;
	
	public RecommendedUserBean() {}
	
	public RecommendedUserBean(String user, Double score, List<String> items) {
		this.user = user;
		this.score = score;
		this.items = items;
	}
	
	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Double getScore() {
		return score;
	}

	public void setScore(Double score) {
		this.score = score;
	}

	public List<String> getItems() {
		return items;
	}
	
	public void setItems(List<String> items) {
		this.items = items;
	}

    @Override
    public String toString() {
        return "RecommendedUserBean{" +
                "user='" + user + '\'' +
                ", score=" + score +
                ", items=" + items +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecommendedUserBean)) return false;

        RecommendedUserBean that = (RecommendedUserBean) o;

        if (items != null ? !items.equals(that.items) : that.items != null) return false;
        if (score != null ? !score.equals(that.score) : that.score != null) return false;
        if (user != null ? !user.equals(that.user) : that.user != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = user != null ? user.hashCode() : 0;
        result = 31 * result + (score != null ? score.hashCode() : 0);
        result = 31 * result + (items != null ? items.hashCode() : 0);
        return result;
    }
}
