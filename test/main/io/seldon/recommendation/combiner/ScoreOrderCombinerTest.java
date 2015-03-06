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
import static io.seldon.trust.impl.jdo.RecommendationPeer.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ScoreOrderCombinerTest {

    RecResultContext emptySet;
    RecResultContext notEnoughInEachSet;
    RecResultContext sufficientSet;
    RecResultContext plentySet;
    List<ItemRecommendationResultSet.ItemRecommendationResult> notEnoughInEachList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> sufficientList;
    List<ItemRecommendationResultSet.ItemRecommendationResult> plentyList;
    ItemRecommendationResultSet.ItemRecommendationResult result1
            = new ItemRecommendationResultSet.ItemRecommendationResult(1L, 1.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result1Greater
            = new ItemRecommendationResultSet.ItemRecommendationResult(1L, 2.5f);
    ItemRecommendationResultSet.ItemRecommendationResult result2
            = new ItemRecommendationResultSet.ItemRecommendationResult(2L, 2.0f);
    ItemRecommendationResultSet.ItemRecommendationResult result3
            = new ItemRecommendationResultSet.ItemRecommendationResult(3L, 3.0f);

    @Before
    public void setup(){
        notEnoughInEachList = Arrays.asList(result1);
        sufficientList = Arrays.asList(result2, result1);
        plentyList = Arrays.asList(result3, result2, result1Greater);

        emptySet = RecResultContext.EMPTY;

        notEnoughInEachSet = new RecResultContext(new ItemRecommendationResultSet(notEnoughInEachList),"notEnough");

        sufficientSet = new RecResultContext(new ItemRecommendationResultSet(sufficientList),"sufficient");
        plentySet = new RecResultContext(new ItemRecommendationResultSet(plentyList),"plenty");
    }



    @Test
    public void isEnoughMethodShouldWorkCorrectly(){
        ScoreOrderCombiner combiner = new ScoreOrderCombiner();
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(RecResultContext.EMPTY)));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(notEnoughInEachSet, emptySet )));
        assertFalse(combiner.isEnoughResults(2, Arrays.asList(notEnoughInEachSet, notEnoughInEachSet)));
        assertTrue(combiner.isEnoughResults(2, Arrays.asList(sufficientSet)));
    }

    @Test
    public void shouldCombineResultsForFirstSuccessful(){
        ScoreOrderCombiner combiner = new ScoreOrderCombiner();
        RecResultContext result = combiner.combine(2, Arrays.asList(sufficientSet));
        assertEquals(sufficientSet.resultSet, result.resultSet);
        result = combiner.combine(2, Arrays.asList(notEnoughInEachSet, sufficientSet));
        assertEquals(sufficientSet.resultSet, result.resultSet);
        result = combiner.combine(2, Arrays.asList(sufficientSet, plentySet));
        assertEquals(plentySet.resultSet, result.resultSet);

    }

}