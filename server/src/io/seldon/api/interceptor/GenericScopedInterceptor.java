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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.service.TokenScope;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import io.seldon.api.logging.ApiLogger;
import io.seldon.api.logging.MDCKeys;
import io.seldon.api.resource.ScopedConsumerBean;
import io.seldon.api.service.AuthorizationServer;

/**
 * Created by: marc on 13/08/2012 at 15:24
 */
abstract public class GenericScopedInterceptor extends HandlerInterceptorAdapter implements ScopedInterceptor {
    private static final Logger logger = Logger.getLogger(GenericScopedInterceptor.class);
    private static final String START_TIME = "startTime";

    @Autowired
    private AuthorizationServer authorizationServer;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        final String requestURI = request.getRequestURI();
        logger.debug("Scoped interceptor for: " + requestURI);
        Date start = new Date();
        request.setAttribute("startTime", start);
        try {
            final ConsumerBean consumerBean = authorise(request);
            final HttpSession session = request.getSession();
            session.setAttribute("consumer", consumerBean);
            return super.preHandle(request, response, handler);
        } catch (APIException e) {
            exceptionResponse(request, response, e);
            String apiKey = request.getServletPath().replaceAll("/", "\\."); //Asumes no user/item ids in path!!!!
            ApiLogger.log(apiKey,start, request, null);
        } catch (Throwable t) {
            logger.error("GenericScopedInterceptor#preHandle: " + t.getMessage(), t);
        }
        return false;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
        Date start = (Date)request.getAttribute(START_TIME);
        final HttpSession session = request.getSession();
        final ConsumerBean consumerBean = (ConsumerBean) session.getAttribute("consumer");
        String apiKey = request.getServletPath().replaceAll("/", "\\."); //Asumes no user/item ids in path!!!!
        ApiLogger.log(apiKey,start, request, consumerBean);
        super.postHandle(request, response, handler, modelAndView);
    }

    protected void exceptionResponse(HttpServletRequest request, HttpServletResponse response, APIException e) throws IOException {
        ErrorBean bean = new ErrorBean(e);
        logger.error("GenericScopedInterceptor#exceptionResponse: " + e.getMessage(), e);
        final ObjectMapper objectMapper = new ObjectMapper();
        response.setContentType("application/json");
        objectMapper.writeValue(response.getOutputStream(), bean);
    }

    private ScopedConsumerBean authorise(HttpServletRequest request) {
        final ScopedConsumerBean scopedConsumerBean = authorizationServer.getConsumer(request);
        if (scopedConsumerBean != null)
        	MDCKeys.addKeysConsumer(scopedConsumerBean);
        final TokenScope.Scope tokenScope = TokenScope.fromString(scopedConsumerBean.getScope());

        final String requestURI = request.getRequestURI();
        final String queryString = request.getQueryString();
        final String requestInfo = requestURI + ", " + queryString;

        if (tokenScope == getScope() && validReferrer(request, scopedConsumerBean)) {
            logger.debug("Scope and referrer valid for request: " + requestInfo);
            return scopedConsumerBean;
        } else {
            logger.warn("Incorrect scope of invalid referrer in request for: " + requestInfo);
            throw new APIException(APIException.METHOD_NOT_AUTHORIZED); // create additional error code?
        }
    }

    private boolean validReferrer(HttpServletRequest request, ScopedConsumerBean scopedConsumerBean) {
        final String referrer = request.getHeader("Referer"); // sic.; incorrect spelling due to spec

        String referringDomain = referrerDomain(referrer);
        logger.debug("Checking " + referringDomain + " (from " + referrer + ") against authorised domains.");

        final String commaSeparatedUrls = scopedConsumerBean.getUrl();
        if (commaSeparatedUrls == null) {
            logger.debug("No URLs specified for consumer: " + scopedConsumerBean.getShort_name() + "; valid referrer.");
            return true;
        }

        final String[] validDomains = StringUtils.split(commaSeparatedUrls, ",");
        for (String validDomain : validDomains) {
            logger.debug("-> Comparing against domain: " + validDomain);
            if (referringDomain.equals(validDomain)) {
                return true;
            }
        }

        logger.warn(referringDomain + " is invalid. Matched against "+commaSeparatedUrls);
        return false;
    }

    private String referrerDomain(String referrer) {
        Pattern topLevelDomain = Pattern.compile(".*?([^.]+\\.[^.]+)");
        Pattern ipAddress = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+");

        if ( StringUtils.isBlank(referrer) ) {
            return "";
        }

        try {
            final URI referrerUri = new URI(referrer);

            final String host = referrerUri.getHost();
            
            if (StringUtils.isBlank(host)){
            	return "";
            }

            final Matcher ipMatcher = ipAddress.matcher(host);
            if (ipMatcher.matches()) {
                return host;
            }

            final Matcher tldMatcher = topLevelDomain.matcher(host);
            if (tldMatcher.matches()) {
                return tldMatcher.group(1);
            }

            return host;
        } catch (URISyntaxException e) {
            return referrer;
        }
    }
}
