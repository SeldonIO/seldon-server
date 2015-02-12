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
import java.util.Map;

/**
 * Created by dylanlentini on 28/02/2014.
 */

public class Impressions implements Serializable {

    private String user;
    private Map<String,Integer> friendsImpressions;


    public Impressions(String user, Map<String,Integer> friendsImpressions)
    {
        this.user = user;
        this.friendsImpressions = friendsImpressions;
    }


    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }


    public Map<String,Integer> getFriendsImpressions() {
        return friendsImpressions;
    }

    public void getFriendsImpressions(Map<String,Integer> friendsImpressions) {
        this.friendsImpressions = friendsImpressions;
    }



    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Impressions that = (Impressions) o;

        if (friendsImpressions.size() == that.friendsImpressions.size())
        {
            for (Map.Entry<String, Integer> thatImpression : that.friendsImpressions.entrySet())
            {
                if (friendsImpressions.containsKey(thatImpression.getKey()))
                {
                    if (!(friendsImpressions.get(thatImpression.getKey()) == thatImpression.getValue()))
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
        }
        else
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = 0;

        for (Map.Entry<String, Integer> thisImpression : friendsImpressions.entrySet())
        {
            result = 31 * result + thisImpression.getKey().hashCode();
            result = 31 * result + thisImpression.getValue().hashCode();
        }

        return result;
    }


}
