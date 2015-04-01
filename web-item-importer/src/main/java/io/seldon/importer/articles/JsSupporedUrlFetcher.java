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
package io.seldon.importer.articles;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import org.apache.log4j.Logger;

public class JsSupporedUrlFetcher implements UrlFetcher {

    private static Logger logger = Logger.getLogger(JsSupporedUrlFetcher.class.getName());

    private final Integer httpGetTimeout;

    public JsSupporedUrlFetcher(Integer httpGetTimeout) {
        this.httpGetTimeout = httpGetTimeout;
    }

    @Override
    public String getUrl(String url) throws Exception {
        long timing_start = System.currentTimeMillis();

        BrowserVersion browserVersion = BrowserVersion.getDefault();
        logger.info("Using user-agent: " + browserVersion.getUserAgent());
        final WebClient webClient = new WebClient(browserVersion);
        webClient.setTimeout(httpGetTimeout);
        final HtmlPage page = webClient.getPage(url);
        long timing_end = System.currentTimeMillis();
        logger.info(String.format("fetched page[%s] in ms[%d]", url, (timing_end - timing_start)));
        return page.asXml();
    }

}
