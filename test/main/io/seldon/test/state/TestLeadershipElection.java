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

import io.seldon.api.state.LeadershipPeer;
import junit.framework.Assert;

import org.junit.Test;


public class TestLeadershipElection  {


	@Test
	public void simpleLeadershipTest() throws InterruptedException
	{
		try
		{
			LeadershipPeer l1 = new LeadershipPeer("localhost","test");
			Thread t1 = new Thread(l1);
			t1.start();
			try
			{
				Thread.sleep(2000);
				boolean isLeader = l1.isLeader();
				Assert.assertTrue(isLeader);
			}
			finally
			{
				l1.setKeepRunning(false);
				t1.interrupt();
				Thread.sleep(1000);
			}
		}
		finally
		{
			
		}
		
	}
	
	
	@Test
	public void simpleLeadershipTestWithTwoClients() throws InterruptedException
	{
		try
		{

			LeadershipPeer l1 = new LeadershipPeer("localhost","test");
			LeadershipPeer l2 = new LeadershipPeer("localhost","test");
			Thread t1 = new Thread(l1);
			t1.start();
			Thread t2 = new Thread(l2);
			t2.start();
			try
			{
				Thread.sleep(2000);
				boolean isLeader1 = l1.isLeader();
				boolean isLeader2 = l2.isLeader();
			Assert.assertTrue(isLeader1 || isLeader2);
			}
			finally
			{
				l1.setKeepRunning(false);
				t1.interrupt();
				l2.setKeepRunning(false);
				t2.interrupt();
				Thread.sleep(10000);
			}
		}
		finally
		{
			
		}
	}
	
	
	@Test
	public void simpleLeadershipChange() throws InterruptedException
	{
		try
		{

			LeadershipPeer l1 = new LeadershipPeer("localhost","test");
			LeadershipPeer l2 = new LeadershipPeer("localhost","test");
			Thread t1 = new Thread(l1);
			t1.start();
			Thread.sleep(2000);//to ensure l1 is leader
			Thread t2 = new Thread(l2);
			t2.start();
			try
			{
				Thread.sleep(2000);
				boolean isLeader1 = l1.isLeader();
				Assert.assertTrue(isLeader1);
				l1.setKeepRunning(false);
				t1.interrupt();
				Thread.sleep(2000);
				isLeader1 = l1.isLeader();
				Assert.assertFalse(isLeader1);
				boolean isLeader2 = l2.isLeader();
				Assert.assertTrue(isLeader2);
			}
			finally
			{
				l2.setKeepRunning(false);
				t2.interrupt();
				Thread.sleep(10000);
			}
		}
		finally
		{
			
		}
	}
	
	
	@Test
	public void simpleLeadershipChangeBut1LeaderPossible() throws InterruptedException
	{
		try
		{

			LeadershipPeer l1 = new LeadershipPeer("localhost","test");
			LeadershipPeer l2 = new LeadershipPeer("localhost","test");
			Thread t1 = new Thread(l1);
			t1.start();
			Thread.sleep(4000);//to ensure l1 is leader
			Thread t2 = new Thread(l2);
			t2.start();
			try
			{
				Thread.sleep(2000);
				boolean isLeader1 = l1.isLeader();
				Assert.assertTrue(isLeader1);
				l1.setKeepRunning(false);
				t1.interrupt();
				Thread.sleep(2000);
				isLeader1 = l1.isLeader();
				Assert.assertFalse(isLeader1);
				boolean isLeader2 = l2.isLeader();
				Assert.assertTrue(isLeader2);
			}
			finally
			{
				l2.setKeepRunning(false);
				t2.interrupt();
				Thread.sleep(10000);
			}
		}
		finally
		{
			
		}
	}
	
	
	
	
}

