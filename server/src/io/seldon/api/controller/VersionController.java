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

import javax.servlet.http.HttpServletRequest;

import io.seldon.api.APIException;
import io.seldon.api.resource.ConsumerBean;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.resource.ResourceBean;
import io.seldon.api.resource.VersionBean;
import io.seldon.api.resource.service.VersionService;
import io.seldon.api.service.ApiLoggerServer;
import io.seldon.api.service.ResourceServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Determine the current API version; query a client for its declared version.
 * <p/>
 * Created by: marc on 10/08/2011 at 13:43
 */
@Controller
public class VersionController {

    private static final Logger logger = LoggerFactory.getLogger(VersionController.class);

    @Autowired
    private VersionBean apiVersion;

    @RequestMapping("/version")
    public @ResponseBody VersionBean getVersion() {
        return apiVersion;
    }

    @RequestMapping("/version/me")
    public @ResponseBody
    ResourceBean clientVersion(HttpServletRequest req) {
        ResourceBean requestBean = ResourceServer.validateResourceRequest(req);
        return getClientVersionBean(requestBean);
    }

    private ResourceBean getClientVersionBean(ResourceBean requestBean) {
        ResourceBean responseBean = requestBean;

        if (requestBean instanceof ConsumerBean) {
            try {
                responseBean = VersionService.getVersion((ConsumerBean) requestBean);
            } catch (APIException e) {
                ApiLoggerServer.log(this, e);
                responseBean = new ErrorBean(e);
            } catch (Exception e) {
                ApiLoggerServer.log(this, e);
                APIException apiEx = new APIException(APIException.GENERIC_ERROR);
                responseBean = new ErrorBean(apiEx);
            }
        }
        
        return responseBean;
    }

}
