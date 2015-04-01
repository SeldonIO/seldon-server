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

import io.seldon.client.beans.ActionBean;
import io.seldon.client.beans.ItemBean;
import io.seldon.client.beans.UserBean;
import io.seldon.client.exception.ApiException;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * These tests are disabled by default as they modify local client databases and don't
 * return the IDs of newly added rows.
 * <p/>
 * To re-enable them, uncomment the @Ignore annotation.
 * <p/>
 * Created by: marc on 01/09/2011 at 12:30
 */
@Ignore
public class ApiClientPostTest extends BaseClientTest {

    // NOTE: user type must be non-null
    @Test
    public void actionPost() throws ApiException {
        ActionBean actionBean = new ActionBean("1", "10", 1);
        // note: this will be ignored
        actionBean.setActionId(1L);
        ActionBean responseBean = apiClient.addAction(actionBean);
        Assert.assertEquals(responseBean, actionBean);
    }

    @Test
    public void itemPost() throws ApiException {
        ItemBean itemBean = new ItemBean("abc", "abc", 0);
        ItemBean responseBean = apiClient.addItem(itemBean);
        Assert.assertEquals(responseBean, itemBean);
    }

    @Test
    public void userPost() throws ApiException {
        UserBean userBean = new UserBean("abc", "abc");
        UserBean responseBean = apiClient.addUser(userBean);
        Assert.assertEquals(responseBean, userBean);
    }

}
