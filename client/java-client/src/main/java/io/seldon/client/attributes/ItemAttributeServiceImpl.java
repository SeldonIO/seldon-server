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

import io.seldon.client.beans.ItemBean;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by: marc on 09/11/2011 at 15:10
 */
public class ItemAttributeServiceImpl extends GenericAttributeServiceImpl<ItemBean> implements ItemAttributeService {

    @Override
    protected Map<String, String> ensureAttributesMap(ItemBean item) {
        Map<String, String> attributesName = item.getAttributesName();
        if (attributesName == null) {
            attributesName = new HashMap<String, String>();
            item.setAttributesName(attributesName);
        }
        return attributesName;
    }

}
