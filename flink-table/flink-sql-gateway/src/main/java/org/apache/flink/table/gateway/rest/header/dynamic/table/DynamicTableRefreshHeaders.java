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

package org.apache.flink.table.gateway.rest.header.dynamic.table;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.message.dynamic.table.DynamicTableRefreshRequestBody;
import org.apache.flink.table.gateway.rest.message.dynamic.table.DynamicTableRefreshResponseBody;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/** Message headers for refresh dynamic table. */
public class DynamicTableRefreshHeaders
        implements SqlGatewayMessageHeaders<
                DynamicTableRefreshRequestBody,
                DynamicTableRefreshResponseBody,
                EmptyMessageParameters> {

    private static final DynamicTableRefreshHeaders INSTANCE = new DynamicTableRefreshHeaders();

    private static final String URL = "/dynamic-tables/refresh";

    @Override
    public HttpMethodWrapper getHttpMethod() {
        return HttpMethodWrapper.POST;
    }

    @Override
    public String getTargetRestEndpointURL() {
        return URL;
    }

    @Override
    public Class<DynamicTableRefreshResponseBody> getResponseClass() {
        return DynamicTableRefreshResponseBody.class;
    }

    @Override
    public HttpResponseStatus getResponseStatusCode() {
        return HttpResponseStatus.OK;
    }

    @Override
    public String getDescription() {
        return "Execute refresh operation on dynamic table.";
    }

    @Override
    public Class<DynamicTableRefreshRequestBody> getRequestClass() {
        return DynamicTableRefreshRequestBody.class;
    }

    @Override
    public EmptyMessageParameters getUnresolvedMessageParameters() {
        return EmptyMessageParameters.getInstance();
    }

    public static DynamicTableRefreshHeaders getInstance() {
        return INSTANCE;
    }
}
