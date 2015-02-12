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

package io.seldon.test.api;

import java.util.concurrent.ConcurrentHashMap;

import io.seldon.api.Util;
import io.seldon.trust.impl.CFAlgorithm;
import junit.framework.Assert;

import org.junit.Test;

import io.seldon.api.AlgorithmServiceImpl;

public class AlgorithmServiceTest {

	@Test
	public void testSetGet() throws CloneNotSupportedException
	{
		
		AlgorithmServiceImpl aService = new AlgorithmServiceImpl();
        ConcurrentHashMap<String, CFAlgorithm> map = new ConcurrentHashMap<String, CFAlgorithm>();
        aService.setAlgorithmMap(map);
        Util.setAlgorithmService(aService);
        
        
        CFAlgorithm alg = new CFAlgorithm();
        alg.setRecTag("tag1");
        Util.getAlgorithmService().setAlgorithmOptions("test", alg);
        
        CFAlgorithm alg2 = Util.getAlgorithmService().getAlgorithmOptions("test", "tag1");
        
        Assert.assertNotNull(alg2);
        Assert.assertEquals(alg.getRecTag(), alg2.getRecTag());

      

	}
}
