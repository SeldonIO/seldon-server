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

package io.seldon.test.trust;

import io.seldon.api.AlgorithmService;
import io.seldon.test.BaseTest;
import io.seldon.trust.impl.CFAlgorithm;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by: marc on 22/11/2011 at 13:46
 */
public class AlgorithmServiceTest extends BaseTest {

    @Autowired
    private CFAlgorithm cfAlgorithm;

    @Autowired
    private AlgorithmService algorithmService;

    @Test
    public void basicRetrievalTest() throws CloneNotSupportedException {
        CFAlgorithm localClone = cfAlgorithm.clone();
        CFAlgorithm consumerAlgorithmOptions = algorithmService.getAlgorithmOptions(consumerBean);
        CFAlgorithm secondConsumerAlgorithmOptions = algorithmService.getAlgorithmOptions(consumerBean);
        // reference comparison
        Assert.assertSame("Multiple retrievals of the same client configuration should return the same instance",
                consumerAlgorithmOptions, secondConsumerAlgorithmOptions);
        Assert.assertNotSame("Retrieved client options should not be the instance instance as the default",
                cfAlgorithm, consumerAlgorithmOptions);
        // test comparability
        Assert.assertEquals("Default algorithm and its clone should be logically equal.", cfAlgorithm, localClone);
    }


}
