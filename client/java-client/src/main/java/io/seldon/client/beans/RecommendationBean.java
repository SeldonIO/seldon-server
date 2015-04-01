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
public class RecommendationBean extends ResourceBean {

    private static final long serialVersionUID = -8945263680601512671L;

    private String item;
    private Long pos;
    private List<UserTrustNodeBean> srcUsers;

    public RecommendationBean() {
    }

    public RecommendationBean(String item, Long pos, List<UserTrustNodeBean> srcUsers) {
        this.item = item;
        this.pos = pos;
        this.srcUsers = srcUsers;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }

    public List<UserTrustNodeBean> getSrcUsers() {
        return srcUsers;
    }

    public void setSrcUsers(List<UserTrustNodeBean> srcUsers) {
        this.srcUsers = srcUsers;
    }

    @Override
    public String toString() {
        return "RecommendationBean{" +
                "item='" + item + '\'' +
                ", pos=" + pos +
                ", srcUsers=" + srcUsers +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecommendationBean)) return false;

        RecommendationBean that = (RecommendationBean) o;

        if (item != null ? !item.equals(that.item) : that.item != null) return false;
        if (pos != null ? !pos.equals(that.pos) : that.pos != null) return false;
        if (srcUsers != null ? !srcUsers.equals(that.srcUsers) : that.srcUsers != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = item != null ? item.hashCode() : 0;
        result = 31 * result + (pos != null ? pos.hashCode() : 0);
        result = 31 * result + (srcUsers != null ? srcUsers.hashCode() : 0);
        return result;
    }
}
