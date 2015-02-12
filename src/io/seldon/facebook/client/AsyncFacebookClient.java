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

package io.seldon.facebook.client;

import com.restfb.*;
import com.restfb.exception.FacebookException;
import com.restfb.exception.FacebookNetworkException;
import com.twitter.finagle.builder.ClientConfig;
import com.twitter.finagle.http.RequestBuilder;
import com.twitter.util.Duration;
import org.jboss.netty.handler.codec.http.*;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.http.Http;
import org.jboss.netty.util.CharsetUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static java.net.HttpURLConnection.*;
import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
/**
 *
 * Basically a hack to get RestFB working with finagle. Instead of returning the FQL results, futures are
 * returned as per the asynchronous model.
 *
 * @author philipince
 *         Date: 17/12/2013
 *         Time: 11:43
 */
@Component
public class AsyncFacebookClient extends DefaultFacebookClient {

    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(AsyncFacebookClient.class);
    final com.twitter.finagle.Service<HttpRequest, HttpResponse> client;
    private final String host;
    private final Integer port;
    private final Boolean isSsl;

    @Autowired
    public AsyncFacebookClient(@Value("${facebook.connection.limit:1}")Integer connectionLimit,
                               @Value("${facebook.connection.core.size:1}")Integer connectionCoreSize,
                               @Value("${facebook.fql.host:localhost}")String host,
                               @Value("${facebook.port:9000}") Integer port,
                               @Value("${facebook.ssl:false}") Boolean ssl) {

        super();
        long before = System.currentTimeMillis();
        this.host = host;
        this.port = port;
        this.isSsl = ssl;
        logger.info("Creating asynchronous facebook client to server at host " + host + " on port " + port + ", using a " +(ssl?" secure ":" non-secure ") + "connection");
        logger.info("Starting " +connectionCoreSize + " connections with the possibility to increase to "+connectionLimit);
        if(ssl){
        ClientBuilder<HttpRequest, HttpResponse,ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes> builder = ClientBuilder.get()
                .codec(
                        Http.get().compressionLevel(6).decompressionEnabled(true)
                )
                .hosts(new InetSocketAddress(host, port))
                .tcpConnectTimeout(Duration.fromSeconds(5))
                .requestTimeout(Duration.fromSeconds(20))
                .hostConnectionLimit(connectionLimit)
                .hostConnectionCoresize(connectionCoreSize)
                        //.logger(Logger.getLogger("finagle"))
                .keepAlive(true)
                .daemon(true)
                .tls(host);
                client = ClientBuilder
                    .safeBuild(builder
                    );
        } else {
            ClientBuilder<HttpRequest, HttpResponse,ClientConfig.Yes, ClientConfig.Yes, ClientConfig.Yes> builder = ClientBuilder.get()
                    .codec(
                            Http.get().compressionLevel(6).decompressionEnabled(true)
                    )
                    .hosts(new InetSocketAddress(host, port))
                    .tcpConnectTimeout(Duration.fromSeconds(5))
                    .requestTimeout(Duration.fromSeconds(20))
                    .hostConnectionLimit(connectionLimit)
                    .hostConnectionCoresize(connectionCoreSize)
                            //.logger(Logger.getLogger("finagle"))
                    .keepAlive(true)
                    .daemon(true);
            client = ClientBuilder
                    .safeBuild(builder
                    );
        }
        logger.info("Took " + (System.currentTimeMillis() - before) + "ms connecting to facebook");
        illegalParamNames.remove("access_token");
    }

    public <T> Future<List<T>> executeFqlQueryAsync(String query, Class<T> objectType, String fbToken, Parameter... parameters){
        Parameter accessParam = Parameter.with("access_token", fbToken);
        Parameter[] newParameters;
        if(parameters==null){
            newParameters = new Parameter[]{accessParam};
        } else {
            newParameters = new Parameter[parameters.length + 1];
            System.arraycopy(parameters,0,newParameters,0,parameters.length);
            newParameters[parameters.length] = accessParam;
        }
        return executeFqlQueryAsync(query, objectType, newParameters);
    }


    public <T> Future<List<T>> executeFqlQueryAsync(String query, Class<T> objectType, Parameter... parameters){
        verifyParameterPresence("query", query);
        verifyParameterPresence("objectType", objectType);

        for (Parameter parameter : parameters)
            if (FQL_QUERY_PARAM_NAME.equals(parameter.name))
                throw new IllegalArgumentException("You cannot specify the '" + FQL_QUERY_PARAM_NAME
                        + "' URL parameter yourself - " + "RestFB will populate this for you with "
                        + "the query you passed to this method.");
        verifyParameterLegality(parameters);
        parameters = parametersWithAdditionalParameter(Parameter.with(FQL_QUERY_PARAM_NAME, query),parameters);

        final String parameterString = toParameterString(parameters);
        HttpRequest request = null;
        try {
            request = RequestBuilder.safeBuildGet(RequestBuilder.create().url(new URL(isSsl?"https":"http", host, port, "/fql?" + parameterString))
                    .setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE)
                    .setHeader(HttpHeaders.Names.ACCEPT_ENCODING, "gzip,deflate,sdch"));
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return couchTwitterFuture(client.apply(request), objectType);
    }

    public <T> List<T> executeFqlQuery(String query, Class<T> objectType, String accessToken, Parameter... parameters){
        try {
            return executeFqlQueryAsync(query,objectType,accessToken,parameters).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            throw new FacebookException("Exception encountered whilst executing query" ,e){
            };
        }
    }

    @Override
    public <T> List<T> executeFqlQuery(String query, Class<T> objectType, Parameter... parameters){
        try {
            return executeFqlQueryAsync(query,objectType,parameters).get();
        } catch (InterruptedException e) {
            return null;
        } catch (ExecutionException e) {
            throw new FacebookException("Exception encountered whilst executing query" ,e){
            };
        }
    }

    private <T> Future<List<T>> couchTwitterFuture(final com.twitter.util.Future<HttpResponse> future, final Class<T> objectType) {
        final Future<? extends HttpResponse> javaFuture = future.toJavaFuture();
        return new Future<List<T>>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return javaFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return javaFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return javaFuture.isDone();
            }

            @Override
            public List<T> get() throws InterruptedException, ExecutionException {
                HttpResponse resp = javaFuture.get();
                return getFbObjFromResponse(resp);
            }

            @Override
            public List<T> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                HttpResponse resp = javaFuture.get(timeout, unit);
                return getFbObjFromResponse(resp);
            }

            private List<T> getFbObjFromResponse(HttpResponse resp) throws ExecutionException, InterruptedException {
                WebRequestor.Response response = new WebRequestor.Response(resp.getStatus().getCode(),resp.getContent().toString(CharsetUtil.UTF_8));
                // If we get any HTTP response code other than a 200 OK or 400 Bad Request
                // or 401 Not Authorized or 403 Forbidden or 404 Not Found or 500 Internal
                // Server Error,
                // throw an exception.
                if(HTTP_MOVED_PERM == response.getStatusCode() || HTTP_MOVED_TEMP == response.getStatusCode()){
                    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                            HttpMethod.GET, resp.getHeader("Location"));
                    return couchTwitterFuture(client.apply(request),objectType).get();
                }
                if (HTTP_OK != response.getStatusCode() && HTTP_BAD_REQUEST != response.getStatusCode()
                        && HTTP_UNAUTHORIZED != response.getStatusCode() && HTTP_NOT_FOUND != response.getStatusCode()
                        && HTTP_INTERNAL_ERROR != response.getStatusCode() && HTTP_FORBIDDEN != response.getStatusCode())
                    throw new FacebookNetworkException("Facebook request failed", response.getStatusCode());

                String json = response.getBody();
                // If the response contained an error code, throw an exception.
                throwFacebookResponseStatusExceptionIfNecessary(json, response.getStatusCode());

                // If there was no response error information and this was a 500 or 401
                // error, something weird happened on Facebook's end. Bail.
                if (HTTP_INTERNAL_ERROR == response.getStatusCode() || HTTP_UNAUTHORIZED == response.getStatusCode())
                    throw new FacebookNetworkException("Facebook request failed", response.getStatusCode());

                return jsonMapper.toJavaList(json, objectType);
            }


        };

    }
}
