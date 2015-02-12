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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import io.seldon.api.APIException;
import io.seldon.api.resource.ErrorBean;
import io.seldon.api.service.TokenScope;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by: marc on 13/08/2012 at 16:53
 */
public class JavascriptScopeInterceptor extends GenericScopedInterceptor implements ScopedInterceptor {

    @Override
    public TokenScope.Scope getScope() {
        return TokenScope.Scope.JAVASCRIPT;
    }

    @Override
    protected void exceptionResponse(HttpServletRequest request, HttpServletResponse response, APIException e) throws IOException {
        ErrorBean bean = new ErrorBean(e);
        String jsonpCallback = request.getParameter("jsonpCallback");
        final ObjectMapper objectMapper = new ObjectMapper();

        response.setContentType("application/json");

        if (StringUtils.isBlank(jsonpCallback)) {
            objectMapper.writeValue(response.getOutputStream(), bean);
        } else {
            JSONPObject jsonp = new JSONPObject(jsonpCallback, bean);
            objectMapper.writeValue(response.getOutputStream(), jsonp);
        }
    }

}
