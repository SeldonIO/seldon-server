/*
 * Seldon -- open source prediction engine
 * =======================================
 * Copyright 2011-2015 Seldon Technologies Ltd and Rummble Ltd (http://www.seldon.io/)
 *
 **********************************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at       
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ********************************************************************************************** 
*/
package io.seldon.importer.articles.dynamicextractors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

public class TestSubDomainDynamicExtractor {

    
    @Test
    public void shouldReturnNullForNormalURL() throws Exception{
        String url = "www.google.com";
        AttributeDetail detail = new AttributeDetail();
        detail.extractor_args = Arrays.asList("1");
        SubDomainDynamicExtractor extr = new SubDomainDynamicExtractor();
        assertNull(extr.extract(detail, url, null));
    }
    
    @Test
    public void shouldReturnFirstPartOfDomainWhenAskedFor() throws Exception{
        String url = "abc.somesite.com/aStory";
        AttributeDetail detail = new AttributeDetail();
        detail.extractor_args = Arrays.asList("1");
        SubDomainDynamicExtractor extr = new SubDomainDynamicExtractor();
        assertEquals("abc", extr.extract(detail, url, null));
    }
    
}
