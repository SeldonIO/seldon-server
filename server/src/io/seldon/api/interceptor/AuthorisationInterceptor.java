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

package io.seldon.api.interceptor;

import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

/**
 * Created by: marc on 13/08/2012 at 15:24
 */
public class AuthorisationInterceptor extends HandlerInterceptorAdapter {
    private static final Logger logger = Logger.getLogger(AuthorisationInterceptor.class);

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        final String requestURI = request.getRequestURI();



        if (logger.isDebugEnabled())
        {
            final Enumeration<String> headerNames = request.getHeaderNames();
        	logger.debug("Interceptor for: " + requestURI);
        	logger.debug("<Headers>");
        	while ( headerNames.hasMoreElements() ) {
        		final String header = headerNames.nextElement();
        		logger.debug(header + " => " + request.getHeader(header));
        	}
        	logger.debug("</Headers>");


        	final String referrer = request.getHeader("Referer"); // sic.; incorrect spelling due to spec
        	logger.debug("Referrer: " + referrer);
        }
        // Authorise on the basis of the referrer header...

        return super.preHandle(request, response, handler);
    }
}
