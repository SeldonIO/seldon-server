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

package io.seldon.api.logging;

import org.apache.log4j.Logger;

import io.seldon.api.statsd.StatsdPeer;

public class CtrLogger {

	private static Logger ctrLogger = Logger.getLogger( "CtrLogger" );
	
	public static void log(boolean isCTR,String client,String algKey,int pos,String clientUserId,String uuid,Long itemId,int numRecentActions,String recsList,String abTestingKey,String recTag)
	{
		if (abTestingKey == null)
			abTestingKey = "A";
		String event = String.format("%s,%s,%s,%d,%s,%s,%s,%d,%s,%s,%s",isCTR ? "CTR" : "IMP",client,algKey,pos,clientUserId,uuid,itemId,numRecentActions,recsList,abTestingKey,recTag);
		ctrLogger.info(event);
		StatsdPeer.logCTR(client, algKey,isCTR,abTestingKey,recTag);
	}

}
