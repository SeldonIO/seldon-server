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

package io.seldon.facebook;

import java.util.HashMap;
import java.util.Map;

import com.restfb.types.User;

/**
 * Created by: marc on 12/01/2012 at 11:25
 */
public class FacebookUserGraph {

    private User user;
    private Map<String, Like> likes = new HashMap<>();
    private Map<String, Like> groups = new HashMap<>();
    private Map<String, FacebookUserGraph> friends;

    public FacebookUserGraph(User user, Map<String, Like> likes) {
        this.user = user;
        this.likes = likes;
    }

    public FacebookUserGraph(User user) {
        this.user = user;
    }

    public FacebookUserGraph() {
    }

    public Map<String, Like> getLikes() {
        return likes;
    }

    public void setLikes(Map<String, Like> likes) {
        this.likes = likes;
    }

    public void setGroups(Map<String, Like> groupsMap) {
        this.groups = groupsMap;
    }

    public Map<String, Like> getGroups() {
        return groups;
    }

    public void setFriends(Map<String, FacebookUserGraph> friends) {
        this.friends = friends;
    }

    public Map<String, FacebookUserGraph> getFriends() {
        return friends;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public String toString() {
        return "FacebookUserGraph{" +
                "user=" + user +
                ", likes=" + likes +
                ", groups=" + groups +
                ", friends=" + friends +
                '}';
    }
    
}
