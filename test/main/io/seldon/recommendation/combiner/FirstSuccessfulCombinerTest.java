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

package io.seldon.recommendation.combiner;

import io.seldon.clustering.recommender.ItemRecommendationResultSet;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FirstSuccessfulCombinerTest {

    ItemRecommendationResultSet emptySet;
    ItemRecommendationResultSet notEnoughInEachSet;
    ItemRecommendationResultSet sufficientSet;
    ItemRecommendationResultSet plentySet;
    List<ItemRecommendationResultSet.ItemRecommendationResult> notEnoughInEachList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> sufficientList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> plentyList;
    ItemRecommendationResultSet.ItemRecommendationResult result1
            = new ItemRecommendationResultSet.ItemRecommendationResult(1L, 1.0f);

    ItemRecommendationResultSet.ItemRecommendationResult result2
            = new ItemRecommendationResultSet.ItemRecommendationResult(2L, 2.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result3
            = new ItemRecommendationResultSet.ItemRecommendationResult(3L, 3.0f);

    @Before
    public void setup(){
        notEnoughInEachList = Arrays.asList(result1);
        sufficientList = Arrays.asList(result2, result1);
        plentyList = Arrays.asList(result3, result2, result1);

        emptySet = new ItemRecommendationResultSet();

        notEnoughInEachSet = new ItemRecommendationResultSet(notEnoughInEachList);

        sufficientSet = new ItemRecommendationResultSet(sufficientList);
        plentySet = new ItemRecommendationResultSet(plentyList);
    }


    @Test
    public void isEnoughMethodShouldWorkCorrectly(){
        FirstSuccessfulCombiner combiner = new FirstSuccessfulCombiner();
        assertFalse(combiner.isEnoughResults(2, new ArrayList<ItemRecommendationResultSet>()));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(notEnoughInEachSet, emptySet )));
        assertTrue(combiner.isEnoughResults(2, Arrays.asList(sufficientSet)));
    }

    @Test
    public void shouldCombineResultsForFirstSuccessful(){
        FirstSuccessfulCombiner combiner = new FirstSuccessfulCombiner();
        ItemRecommendationResultSet result = combiner.combine(2, Arrays.asList(sufficientSet));
        assertEquals(sufficientSet, result);
        result = combiner.combine(2, Arrays.asList(notEnoughInEachSet, sufficientSet));
        assertEquals(sufficientSet, result);
        result = combiner.combine(2, Arrays.asList(sufficientSet, plentySet));
        assertEquals(sufficientSet, result);

    }


}