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

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import io.seldon.importer.articles.dynamicextractors.AttributeDetail;
import io.seldon.importer.articles.dynamicextractors.MultiSelectorDynamicExtractor;
import io.seldon.importer.articles.dynamicextractors.AttributeDetail.SubExtractor;

import java.util.ArrayList;
import java.util.Arrays;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Test;

public class TestMultiSelectorDynamicExtractor {

    @Test
    public void shouldFailIfNoSelectorsMatch() throws Exception{
        Document document = createMock(Document.class);
        MultiSelectorDynamicExtractor extr = new MultiSelectorDynamicExtractor();
        AttributeDetail attrDetail = new AttributeDetail();
        attrDetail.sub_types = new ArrayList<SubExtractor>();
        assertNull(extr.extract(attrDetail, null, document));
        replay(document);
        verify(document);
    }
    
    @Test
    public void shouldOnlyRunFirstExtractorIfItGetsAMatch() throws Exception {
        Document document = createMock(Document.class);
        Elements elements = createMock(Elements.class);
        Element element = createMock(Element.class);
        MultiSelectorDynamicExtractor extr = new MultiSelectorDynamicExtractor();
        AttributeDetail attrDetail = new AttributeDetail();
        SubExtractor extr1 = new SubExtractor();
        SubExtractor extr2 = new SubExtractor();
        extr1.extractor_type = "FirstElementTextValue";
        extr1.extractor_args = Arrays.asList("selector");
        extr2.extractor_type = "FirstElementTextValue";
        extr2.extractor_args = Arrays.asList("selector2");
        attrDetail.sub_types = Arrays.asList(extr1, extr2);
        expect(document.select("selector")).andReturn(elements);
        expect(elements.first()).andReturn(element);
        expect(element.text()).andReturn("notEmpty");
        replay(document, elements, element);
        assertEquals("notEmpty",extr.extract(attrDetail, null, document));
        verify(document, elements, element);        
    }
    
    @Test
    public void shouldRunSecondExtractorIfFirstDoesntMatch() throws Exception {
        Document document = createMock(Document.class);
        Elements elements = createMock(Elements.class);
        Element element = createMock(Element.class);
        Elements elements2 = createMock(Elements.class);
        Element element2 = createMock(Element.class);
        MultiSelectorDynamicExtractor extr = new MultiSelectorDynamicExtractor();
        AttributeDetail attrDetail = new AttributeDetail();
        SubExtractor extr1 = new SubExtractor();
        SubExtractor extr2 = new SubExtractor();
        extr1.extractor_type = "FirstElementTextValue";
        extr1.extractor_args = Arrays.asList("selector");
        extr2.extractor_type = "FirstElementTextValue";
        extr2.extractor_args = Arrays.asList("selector2");
        attrDetail.sub_types = Arrays.asList(extr1, extr2);
        expect(document.select("selector")).andReturn(elements);
        expect(elements.first()).andReturn(element);
        expect(element.text()).andReturn("");
        expect(document.select("selector2")).andReturn(elements2);
        expect(elements2.first()).andReturn(element2);
        expect(element2.text()).andReturn("notEmpty");
        replay(document, elements, element, elements2, element2);
        assertEquals("notEmpty",extr.extract(attrDetail, null, document));
        verify(document, elements, element, elements2, element2);        
    }
    
}
