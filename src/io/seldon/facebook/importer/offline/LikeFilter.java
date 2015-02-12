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

package io.seldon.facebook.importer.offline;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import io.seldon.facebook.Like;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;

/**
 * Created by: marc on 15/05/2012 at 17:14
 */
public class LikeFilter {
    private List<Predicate> predicates = new LinkedList<Predicate>();

    public LikeFilter addPredicate(Predicate predicate) {
        predicates.add(predicate);
        return this;
    }

    public static LikeFilter withPredicates(Predicate... predicates) {
        LikeFilter likeFilter = new LikeFilter();
        likeFilter.predicates.addAll(Arrays.asList(predicates));
        return likeFilter;
    }

    public Collection<Like> filterLikes(Collection<Like> likes) {
        List<Like> result = new LinkedList<Like>(likes);
        for (Predicate predicate : predicates) {
            CollectionUtils.filter(result, predicate);
        }
        return result;
    }

    public List<Predicate> getPredicates() {
        return predicates;
    }

}
