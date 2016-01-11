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
package io.seldon.api.caching;

import io.seldon.general.Action;

public class ActionLogEntry {

	public String client;
	public String rectag;
	public long userid;
	public long itemid;
	public int type;
	public String value;
	public String client_userid;
	public String client_itemid;
	
	public ActionLogEntry(){}
	
	public ActionLogEntry(String client, String rectag, long userid,
			long itemid, int type, String value, String client_userid,
			String client_itemid) {
		super();
		this.client = client;
		this.rectag = rectag;
		this.userid = userid;
		this.itemid = itemid;
		this.type = type;
		this.value = value;
		this.client_userid = client_userid;
		this.client_itemid = client_itemid;
	}
	
	public ActionLogEntry(String client,Action a)
	{
		this.client = client;
		this.rectag = "";
		this.userid = a.getUserId();
		this.itemid = a.getItemId();
		this.type = a.getType();
		this.value = ""+a.getValue();
		this.client_userid = a.getClientUserId();
		this.client_itemid = a.getClientItemId();
	}
	
	public Action toAction()
	{
		return new Action(0L, userid, itemid, type, 1, null, Double.parseDouble(value), client_userid, client_itemid);
	}
	
	
}
