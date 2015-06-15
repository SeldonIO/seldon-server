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

package io.seldon.recommendation.filters;

import io.seldon.general.ItemStorage;
import io.seldon.recommendation.ItemIncluder;

import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author firemanphil
 *         Date: 19/11/14
 *         Time: 11:49
 */
@Component
public class MostPopularIncluder implements ItemIncluder {

    @Autowired
    private ItemStorage retriever;


    @Override
    public FilteredItems generateIncludedItems(String client, Set<Integer> dimensions, int numItems) {
        // first stab at this: lets return, say, the top 200 items.
        return  retriever.retrieveMostPopularItems(client,numItems,dimensions);
    }
}
