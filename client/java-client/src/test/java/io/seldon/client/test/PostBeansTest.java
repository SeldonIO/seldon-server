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

import org.junit.Ignore;
import org.junit.Test;

/**
 * These tests are disabled by default as they modify local client databases and don't
 * return the IDs of newly added rows.
 * <p/>
 * To re-enable them, uncomment the @Ignore annotation.
 * <p/>
 * Created by: marc on 01/09/2011 at 11:45
 */
@Ignore
public class PostBeansTest extends BaseBeansTest {
	
	public final static long LOOP_LIMIT = 100;

    // NOTE: user type must be non-null
    @Test
    public void actionPost() {
        ActionBean actionBean = new ActionBean("1", "10", 1);
        // note: this will be ignored
        actionBean.setActionId(1L);
        apiService.addAction(actionBean);
    }

    @Test
    public void itemPost() {
        ItemBean itemBean = new ItemBean("abc", "abc", 0);
        apiService.addItem(itemBean);
    }

    @Test
    public void userPost() {
        UserBean userBean = new UserBean("abc", "abc");
        apiService.addUser(userBean);
    }
    
    @Test
    public void ItemsUpdate() {
    	for(int i=0; i<LOOP_LIMIT; i++) {
    		ItemBean bean = new ItemBean("item_" + i,"item_" + i,0);
    		apiService.updateItem(bean);
    	}
    }
    
    
    @Test
    public void UsersUpdate() {
    	for(int i=0; i<LOOP_LIMIT; i++) {
    		UserBean bean = new UserBean("user" + i,"user" + i);
    		apiService.updateUser(bean);
    	}
    }
    
    
    @Test
    public void ActionsInsert() {
    	for(int i=0; i<LOOP_LIMIT; i++) {
    		ActionBean bean = new ActionBean("user" + i,"item_" + i,1);
    		apiService.addAction(bean);
    	}
    }
}
