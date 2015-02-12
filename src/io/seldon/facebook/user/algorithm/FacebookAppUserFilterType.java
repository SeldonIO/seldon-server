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

package io.seldon.facebook.user.algorithm;


import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author philipince
 *         Date: 07/03/2014
 *         Time: 10:25
 */
public enum FacebookAppUserFilterType {
    NONE(""),
    REMOVE_APP_USERS(" and is_app_user=0"),
    REMOVE_NON_APP_USERS(" and is_app_user=1");

    String querySegment;

    FacebookAppUserFilterType(String querySegment){
        this.querySegment = querySegment;
    }

    public static FacebookAppUserFilterType fromString(String argument){
        if(argument==null){
            return NONE;
        }

        if(argument.toLowerCase().equals("filter_app_users")){
            return REMOVE_APP_USERS;
        }

        if(argument.toLowerCase().equals("retain_app_users")){
            return REMOVE_NON_APP_USERS;
        }
        return NONE;
    }

    public static FacebookAppUserFilterType fromQueryParams(Multimap<String, String> dict){
        Collection<String> appUserFilterTypeEntry = null;
        if((appUserFilterTypeEntry = dict.get("app_user_filter"))!=null){
            if(!appUserFilterTypeEntry.isEmpty()){
                Iterator<String> iter = appUserFilterTypeEntry.iterator();

                    return fromString(iter.next());

            }
        }
            return  NONE;
    }

    public String toQuerySegment() {
        return querySegment;
    }
}
