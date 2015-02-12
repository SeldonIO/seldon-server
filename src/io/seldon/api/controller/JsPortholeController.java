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

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Date;
import java.util.UUID;

import io.seldon.api.logging.ApiLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/session")
public class JsPortholeController {
    private static final Logger logger = LoggerFactory.getLogger(JsPortholeController.class);


    private static final String RL_COOKIE_ID = "rlId";
    private static final int COOKIE_MAX_AGE = 50000000;

    @RequestMapping("/proxy")
    public ModelAndView portholeProxy() {
        return new ModelAndView("porthole-proxy");
    }

    @RequestMapping("/id")
    public
    @ResponseBody
    ModelAndView setCookie(HttpServletRequest request, HttpServletResponse response,
                           @RequestParam(value = "localId", required = false) String localId,
                           @RequestParam("proxy") String proxy) {
        Date start = new Date();
        String userId = ensureCookie(request, response, localId);
        final ModelAndView mav = new ModelAndView("porthole-session");
        mav.addObject("uuid", userId);
        mav.addObject("proxy", proxy);
        mav.addObject("closeImmediately", localId != null);
        ApiLogger.log("id",start, request, null);
        return mav;
    }

    /**
     *
     * @param request ...
     * @param response ...
     * @param localId if non-null, use this local id instead of generating a {@link UUID}.
     *                Typically this will be used to propagate client-specific cookies where browser privacy issues have
     *                blocked the server-side setting.
     * @return
     */
    private String ensureCookie(HttpServletRequest request, HttpServletResponse response, String localId) {
        final Cookie[] cookies = request.getCookies();
        String uuid = null;
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals(RL_COOKIE_ID)) {
                    uuid = cookie.getValue();
                }
            }
        }
        if (uuid == null) {
            if ( localId != null ) {
                logger.info("Using local ID for porthole session: " + localId);
                uuid = localId;
            } else {
                uuid = UUID.randomUUID().toString();
            }
            final Cookie cookie = new Cookie(RL_COOKIE_ID, uuid);
            cookie.setMaxAge(COOKIE_MAX_AGE);
            response.addCookie(cookie);
            response.addHeader("P3P", "CP=\"IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT\"");
        }
        return uuid;
    }

}