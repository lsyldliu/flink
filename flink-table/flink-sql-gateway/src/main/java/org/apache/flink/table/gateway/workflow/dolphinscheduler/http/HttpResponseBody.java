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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** Http response body. */
public class HttpResponseBody {

    private final Integer code;

    private final String msg;

    private final Object data;

    private final Boolean failed;

    private final Boolean success;

    @JsonCreator
    public HttpResponseBody(
            @JsonProperty("code") Integer code,
            @JsonProperty("msg") String msg,
            @JsonProperty("data") Object data,
            @JsonProperty("failed") Boolean failed,
            @JsonProperty("success") Boolean success) {
        this.code = code;
        this.msg = msg;
        this.data = data;
        this.failed = failed;
        this.success = success;
    }

    public Integer getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }

    public Object getData() {
        return data;
    }

    public Boolean getFailed() {
        return failed;
    }

    public Boolean getSuccess() {
        return success;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HttpResponseBody that = (HttpResponseBody) o;
        return Objects.equals(code, that.code)
                && Objects.equals(msg, that.msg)
                && Objects.equals(data, that.data)
                && Objects.equals(failed, that.failed)
                && Objects.equals(success, that.success);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, msg, data, failed, success);
    }

    @Override
    public String toString() {
        return "HttpResponseBody{"
                + "code="
                + code
                + ", msg='"
                + msg
                + '\''
                + ", data="
                + data
                + ", failed="
                + failed
                + ", success="
                + success
                + '}';
    }
}
