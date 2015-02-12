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

package io.seldon.test.state;

import io.seldon.api.state.ZkCuratorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.netflix.curator.test.KillSession;

public class TestZkKillSession {

	static final String consumerName = "test";
	static final String zkServer = "localhost";

	
	
	@Before
	public void createPath() throws Exception
	{
		new ZkCuratorHandler(zkServer);
	}
	
	@After
	public void tearDown()
	{
		ZkCuratorHandler.shutdown();
	}
	
	@Test
	public void testABTestUpdaterSession() throws Exception
	{
		KillSession.kill(ZkCuratorHandler.getPeer().getCurator().getZookeeperClient().getZooKeeper(), zkServer);
		
		Thread.sleep(20000);
	}

}
