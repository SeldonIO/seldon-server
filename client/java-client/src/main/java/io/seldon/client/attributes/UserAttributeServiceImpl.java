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
package io.seldon.client.attributes;

import io.seldon.client.beans.UserBean;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by: marc on 10/11/2011 at 14:12
 */
public class UserAttributeServiceImpl extends GenericAttributeServiceImpl<UserBean> implements UserAttributeService {

    @Override
    protected Map<String, String> ensureAttributesMap(UserBean user) {
        Map<String, String> attributesName = user.getAttributesName();
        if (attributesName == null) {
            attributesName = new HashMap<String, String>();
            user.setAttributesName(attributesName);
        }
        return attributesName;
    }

    @Override
    public void enableFacebookConnect(UserBean userBean, String facebookId, String facebookToken) {
        addBooleanAttribute(userBean, "facebook", true);
        addStringAttribute(userBean, "facebookId", facebookId);
        addStringAttribute(userBean, "facebookToken", facebookToken);
    }

    @Override
    public void disableFacebookConnect(UserBean userBean) {
        addBooleanAttribute(userBean, "facebook", false);
    }

}
