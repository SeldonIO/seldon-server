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
package io.seldon.client.test;

import io.seldon.client.attributes.ItemAttributeService;
import io.seldon.client.attributes.UserAttributeService;
import io.seldon.client.beans.ItemBean;
import io.seldon.client.beans.UserBean;
import io.seldon.client.exception.ApiException;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: marc on 09/11/2011 at 15:09
 */
@Ignore
public class AttributeTest extends BaseClientTest {

    @Autowired
    private ItemAttributeService itemAttributeService;

    @Autowired
    private UserAttributeService userAttributeService;

    private ItemBean item;

    @Before
    public void setup() {
        String dateId = new Date().toString();
        item = new ItemBean();
        item.setId(dateId);
        item.setName(dateId);
    }

    @Test
    public void basicTest() throws ApiException {
        item.setType(2); 
        itemAttributeService.addBooleanAttribute(item, "available", true);
        itemAttributeService.addDateAttribute(item, "published", new Date());
        // valid values: { "pop", "rock", "dance", "indie" }
        itemAttributeService.addEnumerationAttribute(item, "genre", "pop");
        itemAttributeService.addStringAttribute(item, "lyrics", "la la la");
        itemAttributeService.addNumericalAttribute(item, "clicks", 10L);
        List<String> tags = new LinkedList<String>();
        tags.add("abc");
        tags.add("def");
        itemAttributeService.addStringAttributes(item, "tag", tags);
        System.out.println(item.getAttributesName());
        apiClient.addItem(item);
    }

    @Test
    public void fbTest() throws ApiException {
        UserBean userBean = new UserBean("foof1", "bard");
        userAttributeService.enableFacebookConnect(userBean, "1234", "5678");
        System.out.println(userBean);
        apiClient.addUser(userBean);
    }

}
