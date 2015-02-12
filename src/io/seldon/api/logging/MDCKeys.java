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

import io.seldon.api.resource.ConsumerBean;
import org.apache.log4j.MDC;

public class MDCKeys {

	public static final String MDC_CONSUMER_KEY = "consumer";
	public static final String MDC_USER_KEY = "user";
	public static final String MDC_ITEM_KEY = "item";
	
	public static void addKeysConsumer(ConsumerBean consumer)
	{
		addKeys(consumer, "", "");
	}
	
	public static void addKeysUser(ConsumerBean consumer,String userId)
	{
		addKeys(consumer, userId, "");
	}
	public static void addKeysItem(ConsumerBean consumer,String itemId)
	{
		addKeys(consumer, "", itemId);
	}
	
	public static void addKeys(ConsumerBean consumer,String userId,String itemId)
	{
		addKeys(consumer, userId, itemId,null);
	}
	
	public static void addKeys(ConsumerBean consumer,String userId,String itemId,String recTag)
	{
		if (consumer != null)
		{
			if (recTag != null)
				MDC.put(MDC_CONSUMER_KEY, consumer.getShort_name()+":"+recTag);
			else
				MDC.put(MDC_CONSUMER_KEY, consumer.getShort_name());
		}
		else
			MDC.put(MDC_CONSUMER_KEY, "");

		if (userId != null)
			MDC.put(MDC_USER_KEY, userId);
		else
			MDC.put(MDC_USER_KEY, "");

		if (itemId != null)
			MDC.put(MDC_ITEM_KEY, itemId);
		else
			MDC.put(MDC_ITEM_KEY, "");
	}
}
