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

import io.seldon.general.Interaction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author philipince
 *         Date: 05/02/2014
 *         Time: 09:46
 */
public class InteractionsBean extends ResourceBean {
    private String userId;
    private List<String> friendIds;
    private Integer type;
    private Date date;
    private Integer subType;
    Map<String,String> algParams;

    public InteractionsBean(){}

    public InteractionsBean(String userId, List<String> friendIds, Integer type, Integer subType) {
        super();
        this.userId = userId;
        this.friendIds = friendIds;
        this.type = type;
        this.subType = subType;
    }

    public InteractionsBean(String userId, List<String> friendIds, Integer type, Integer subType, Map<String,String> algParams, Date date) {
        super();
        this.userId = userId;
        this.friendIds = friendIds;
        this.type = type;
        this.date = date;
        this.subType = subType;
        this.algParams = algParams;
    }


    public String getUserId() {
        return userId;
    }


    public void setUserId(String userId) {
        this.userId = userId;
    }


    public List<String> getFriendIds() {
        return friendIds;
    }


    public void setFriendIds(List<String> friendIds) {
        this.friendIds = friendIds;
    }


    public Integer getType() {
        return type;
    }


    public void setType(Integer type) {
        this.type = type;
    }


    public Date getDate() {
        return date;
    }


    public void setDate(Date date) {
        this.date = date;
    }


    public Integer getSubType() {
        return subType;
    }


    public void setSubType(Integer subType) {
        this.subType = subType;
    }

    public Map<String,String> getAlgParams() {
        return algParams;
    }


    public void setAlgParams(Map<String,String> algParams) {
        this.algParams = algParams;
    }


    @Override
    public String toKey() {
        return userId+":"+friendIds+":"+type+":"+subType;
    }

    public List<Interaction> toInteractions() {
        List<Interaction> toReturn = new ArrayList<>(friendIds.size());
        for(String friendId : friendIds){
            toReturn.add(new Interaction(userId, friendId, type, subType, date, null));
        }
        return toReturn;
    }
}
