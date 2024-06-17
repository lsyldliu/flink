/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.gateway.workflow.dolphinscheduler.http;

import org.apache.flink.table.gateway.workflow.dolphinscheduler.JSONUtils;

import okhttp3.FormBody;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RequestClient {

    private static final Logger LOG = LoggerFactory.getLogger(RequestClient.class);

    private final OkHttpClient httpClient;
    private final String baseUrl;

    public RequestClient(String baseUrl) {
        this.baseUrl = baseUrl;
        this.httpClient =
                new OkHttpClient.Builder()
                        .connectTimeout(5, TimeUnit.MINUTES) // connect timeout
                        .writeTimeout(5, TimeUnit.MINUTES) // write timeout
                        .readTimeout(5, TimeUnit.MINUTES)
                        .build();
    }

    public HttpResponse get(String url, Map<String, String> headers, Map<String, Object> params) {
        String requestUrl = buildUrlWithParams(baseUrl + url, params);
        headers = createHeadersWithDefaultContent(headers);
        LOG.info("GET request to {}, Headers: {}, Params: {}", requestUrl, headers, params);
        Request request =
                new Request.Builder().url(requestUrl).headers(Headers.of(headers)).get().build();
        return executeRequest(request);
    }

    public HttpResponse post(String url, Map<String, String> headers, Map<String, Object> params) {
        String requestUrl = buildUrlWithParams(baseUrl + url, params);
        headers = createHeadersWithDefaultContent(headers);

        LOG.info("POST request to {}, Headers: {}, Params: {}", requestUrl, headers, params);
        Request request =
                new Request.Builder()
                        .headers(Headers.of(headers))
                        .url(requestUrl)
                        .post(new FormBody.Builder().build())
                        .build();
        return executeRequest(request);
    }

    public HttpResponse put(String url, Map<String, String> headers, Map<String, Object> params) {
        String requestUrl = buildUrlWithParams(baseUrl + url, params);
        headers = createHeadersWithDefaultContent(headers);

        LOG.info("PUT request to {}, Headers: {}, Params: {}", requestUrl, headers, params);
        Request request =
                new Request.Builder()
                        .headers(Headers.of(headers))
                        .url(requestUrl)
                        .put(new FormBody.Builder().build())
                        .build();
        return executeRequest(request);
    }

    public HttpResponse delete(
            String url, Map<String, String> headers, Map<String, Object> params) {
        String requestUrl = buildUrlWithParams(baseUrl + url, params);
        headers = createHeadersWithDefaultContent(headers);

        LOG.info("DELETE request to {}, Headers: {}, Params: {}", requestUrl, headers, params);
        Request request =
                new Request.Builder().headers(Headers.of(headers)).url(requestUrl).delete().build();

        return executeRequest(request);
    }

    private HttpResponse executeRequest(Request request) {
        try (Response response = this.httpClient.newCall(request).execute()) {
            int responseCode = response.code();
            HttpResponseBody responseData = null;
            if (response.body() != null) {
                responseData = parseResponseBody(response.body());
            }
            HttpResponse httpResponse = new HttpResponse(responseCode, responseData);
            LOG.info("HTTP response: {}", httpResponse);
            return httpResponse;
        } catch (IOException e) {
            LOG.error("HTTP request occurred exception.", e);
            // Possibly wrap and rethrow the exception, depending on your error handling strategy
            throw new UncheckedIOException("HTTP request occurred exception.", e);
        }
    }

    private HttpResponseBody parseResponseBody(ResponseBody body) throws IOException {
        return JSONUtils.parseObject(body.string(), HttpResponseBody.class);
    }

    private Map<String, String> createHeadersWithDefaultContent(Map<String, String> headers) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        // Only if there's no Content-Type yet, set the default one
        headers.putIfAbsent("Content-Type", Constants.REQUEST_CONTENT_TYPE);
        return headers;
    }

    private String buildUrlWithParams(String url, Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return url;
        }
        String paramString =
                params.entrySet().stream()
                        .filter(e -> Objects.nonNull(e.getValue()))
                        .map(e -> e.getKey() + Constants.EQUAL_MARK + e.getValue())
                        .collect(Collectors.joining(Constants.AND_MARK));
        return url + "?" + paramString;
    }
}
