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

package io.seldon.db.jdo;

import org.apache.log4j.Logger;

public class NestedTransactionCounter  extends ThreadLocal {

	private static Logger logger = Logger.getLogger( NestedTransactionCounter.class.getName() );
	
	public int startTransaction()
	{
		Integer count = (Integer) super.get();
		if (count == null)
			count = new Integer(1);
		else
			count = new Integer(count + 1);
		super.set(count);
		return count;
	}
	
	public int endTransaction()
	{
		Integer count = (Integer) super.get();
		if (count == null)
		{
            final String message = "Count is null on end transaction";
            logger.error(message, new Exception(message));
			count = 0;
		}
		else
			count = new Integer(count -1);
		if (count < 0)
		{
            final String message = "Count is less than zero " + count;
            logger.error(message, new Exception(message));
			count = 0;
		}
		super.set(count);
		return count;
	}
	
	
	
}
