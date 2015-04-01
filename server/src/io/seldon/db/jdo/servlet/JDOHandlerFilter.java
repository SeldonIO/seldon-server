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

package io.seldon.db.jdo.servlet;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import io.seldon.db.jdo.JDOFactory;
import org.apache.log4j.Logger;

public class JDOHandlerFilter implements Filter
{
    private static Logger logger = Logger.getLogger( JDOHandlerFilter.class.getName() );


    public void init(FilterConfig config) throws ServletException
    {
    }

    public void destroy()
    {
    }

    public void doFilter( ServletRequest request,
                           ServletResponse response,
                           FilterChain chain) throws IOException, ServletException
    {
        
        try
        {
            chain.doFilter( request, response );
        }
        finally
        {
            JDOFactory.cleanupPM();
        }
    }
	

}
