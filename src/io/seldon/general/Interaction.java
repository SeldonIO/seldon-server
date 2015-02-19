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

package io.seldon.general;

import java.io.Serializable;
import java.util.*;

public class Interaction implements Serializable {

    private String id;
    private String user1Id;
    private String user2FbId;
    private int type;
    private int subType;
    private int count;
    private int parameterId;
    private Date date;
    private Set<InteractionEvent> interactionEvents = new HashSet<>();

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getParameterId() {
        return parameterId;
    }

    public void setParameterId(int parameterId) {
        this.parameterId = parameterId;
    }

    public Interaction(String user1Id, String user2FbId, int type, int subType, Date date, Set<InteractionEvent> events) {
        super();
        this.user1Id = user1Id;
        this.user2FbId = user2FbId;
        this.type = type;
        this.subType = subType;
        this.date = date;
        this.count = 1;
        this.interactionEvents = events;
    }
    
    public String getUser1Id() {
        return user1Id;
    }
    public String getUser2FbId() {
        return user2FbId;
    }
    public int getType() {
        return type;
    }
    public int getSubType() {
        return subType;
    }
    public Date getDate() {
        return date;
    }

    public void setUser1Id(String u1) {
        this.user1Id = u1;
    }

    public void setUser2FbId(String u2) {
        this.user2FbId = u2;
    }

    public void setType(int type) {
        this.type = type;
    }
    
    public void setSubType(int subType) {
        this.subType = subType;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Set<InteractionEvent> getInteractionEvents() {
        return interactionEvents;
    }

    public void setInteractionEvents(Set<InteractionEvent> interactionEvents) {
        this.interactionEvents = interactionEvents;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Interaction)) return false;

        Interaction that = (Interaction) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
