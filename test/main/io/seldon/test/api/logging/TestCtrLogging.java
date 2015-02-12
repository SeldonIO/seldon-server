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

package io.seldon.test.api.logging;

import io.seldon.api.logging.CtrFullLogger;
import org.junit.Test;

import io.seldon.api.logging.CtrLogger;

public class TestCtrLogging {

	@Test
	public void testCTRAlgorithmLogging()
	{
		CtrLogger.log(true,"test", "CLUSTER_COUNTS", 2, "1234", "uuid1",3456L,2,"",null,null);
		CtrLogger.log(false,"test", "CLUSTER_COUNTS", 2, "1234", "uuid1",3456L,3,"2:3:4","B",null);
		CtrLogger.log(false,"test", "CLUSTER_COUNTS", 2, "1234", "uuid1",null,4,"4:5:6","C",null);
		CtrLogger.log(false,"test", "CLUSTER_COUNTS", 2, "1234", "uuid1",null,4,"4:5:6","C","mobile");
	}

	@Test
	public void testCTRLogging()
	{
		CtrFullLogger.log(true, "test", "user-1234", "item-1234", null);
		CtrFullLogger.log(false, "test", "user-1234", "item-1234",null);
	}
	
}
