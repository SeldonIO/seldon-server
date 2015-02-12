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

package io.seldon.api.handler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.SimpleMappingExceptionResolver;

/**
 * Created by: marc on 31/07/2012 at 17:31
 * <p/>
 * See http://static.springsource.org/spring/docs/3.0.x/spring-framework-reference/html/mvc.html#d0e29676
 * <p/>
 * Passes through to {@link SimpleMappingExceptionResolver} after logging.
 */
public class ApiHandlerExceptionResolver extends SimpleMappingExceptionResolver {
    private static final Logger logger = Logger.getLogger(ApiHandlerExceptionResolver.class);

    @Override
    protected ModelAndView doResolveException(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        String requestURI = request.getRequestURI();
        String queryString = request.getQueryString();
        logger.error("Uncaught exception: " + ex.getMessage() + " for " + requestURI + " with query " + queryString, ex);
        return super.doResolveException(request, response, handler, ex);
    }

}
