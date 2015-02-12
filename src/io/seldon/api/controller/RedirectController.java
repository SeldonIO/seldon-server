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

package io.seldon.api.controller;

import com.google.common.net.InternetDomainName;
import io.seldon.api.APIException;
import io.seldon.api.resource.ScopedConsumerBean;
import io.seldon.api.resource.service.business.ActionBusinessService;
import io.seldon.api.service.AuthorizationServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author philipince
 *         Date: 30/10/2013
 *         Time: 14:23
 */
@Controller
@RequestMapping("/redirect")
public class RedirectController {

    private ConcurrentMap<String, String> hostToTopPrivateDomain = new ConcurrentHashMap<String, String>();

    private static final Logger logger = LoggerFactory.getLogger(RedirectController.class);
    @Autowired
    private ActionBusinessService actionBusinessService;

    public RedirectController(){
        hostToTopPrivateDomain.put("localhost","localhost");
    }

    @RequestMapping(method = RequestMethod.GET)
    public String redirect(HttpServletRequest request, HttpServletResponse response,
                           @RequestParam(value = "consumer_key", required = true) String consumerKey,
                           @RequestParam(value = "url", required = true) String url,
                           @RequestParam(value = "rlctu", required = true) String inviter,
                           @RequestHeader(value= "user-agent", required = false) String userAgent)

    {
        ScopedConsumerBean consumer = AuthorizationServer.getConsumer(request);

        URI normalisedUrl;
        try {
            normalisedUrl = actionBusinessService.redirectToMgmInviteURL(consumer, url, inviter, userAgent);
            Cookie cookie = new Cookie("rlId_ctu", inviter);
            URI requestURI = new URI(request.getRequestURL().toString());
            String topPrivateDomain = hostToTopPrivateDomain.get(requestURI.getHost());
            if(topPrivateDomain==null){
                topPrivateDomain = InternetDomainName.from(requestURI.getHost()).topPrivateDomain().toString();
                hostToTopPrivateDomain.put(requestURI.getHost(), topPrivateDomain);
            }
            if(topPrivateDomain.equals(requestURI.getHost())){
                cookie.setPath("/");
            } else {
                cookie.setPath("."+topPrivateDomain);
            }
            response.addCookie(cookie);
            boolean allowed = false;
            if(consumer.getUrl()==null){
                allowed = true;
            }else{
                for(String allowedUrl : consumer.getUrl().split(",")){
                    if(!allowed && normalisedUrl.getHost().contains(allowedUrl)){
                        allowed=true;
                    }
                }
            }
            if(!allowed) throw new APIException(APIException.METHOD_NOT_AUTHORIZED);
        } catch (URISyntaxException e) {
            logger.warn("Couldn't read url when doing MGM redirection: "+url, e);
            throw new APIException(APIException.GENERIC_ERROR);
        }
        logger.info("Redirecting to "+ normalisedUrl.toString());
        return "redirect:"+normalisedUrl.toString();
    }

    public static void main(String args[]){
        System.out.println(InternetDomainName.from("api.rummblelabs.com/redirect").topPrivateDomain().name());
    }
}