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

import java.util.Date;

import com.google.common.collect.Multimap;

public class InteractionBean extends ResourceBean{

    private String user1;
    private String user2;
    private Integer type;
    private Date date;
    private Integer subType;
    Multimap<String,String> algParams;
    
    
    public InteractionBean(String user1, String user2, Integer type, Integer subType, Date date) {
        super();
        this.user1 = user1;
        this.user2 = user2;
        this.type = type;
        this.date = date;
        this.subType = subType;
    }
    
    public InteractionBean(String user1, String user2, Integer type, Integer subType, Multimap<String,String> algParams, Date date) {
        super();
        this.user1 = user1;
        this.user2 = user2;
        this.type = type;
        this.date = date;
        this.subType = subType;
        this.algParams = algParams;
    }


    public String getUser1() {
        return user1;
    }


    public void setUser1(String user1) {
        this.user1 = user1;
    }


    public String getUser2() {
        return user2;
    }


    public void setUser2(String user2) {
        this.user2 = user2;
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
    
    public Multimap<String,String> getAlgParams() {
        return algParams;
    }


    public void setAlgParams(Multimap<String,String> algParams) {
        this.algParams = algParams;
    }


    @Override
    public String toKey() {
        return user1+":"+user2+":"+type+":"+subType;
    }

    public Interaction toInteraction(String user1) {
        return new Interaction(user1, user2, type, subType, date, null);
    }
}
