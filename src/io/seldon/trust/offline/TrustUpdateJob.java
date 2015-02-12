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

package io.seldon.trust.offline;

import io.seldon.trust.impl.TrustNetworkSupplier;


public class TrustUpdateJob implements Runnable {

	protected String client;
	protected long id;
	protected int type;
	protected TrustNetworkSupplier.CF_TYPE trustType;
	
	public TrustUpdateJob(String client,long id,int type,TrustNetworkSupplier.CF_TYPE trustType)
	{
		this.client = client;
		this.id = id;
		this.type = type;
		this.trustType = trustType;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	

}
