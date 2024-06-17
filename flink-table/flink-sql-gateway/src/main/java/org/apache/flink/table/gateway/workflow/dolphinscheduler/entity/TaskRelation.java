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

package org.apache.flink.table.gateway.workflow.dolphinscheduler.entity;

/** Task relation info. */
public class TaskRelation {

    /** name */
    private String name;

    /** pre task code */
    private long preTaskCode;

    /** pre node version */
    private int preTaskVersion;

    /** post task code */
    private long postTaskCode;

    /** post node version */
    private int postTaskVersion;

    /** condition type */
    private ConditionType conditionType;

    public String getName() {
        return name;
    }

    public long getPreTaskCode() {
        return preTaskCode;
    }

    public int getPreTaskVersion() {
        return preTaskVersion;
    }

    public long getPostTaskCode() {
        return postTaskCode;
    }

    public int getPostTaskVersion() {
        return postTaskVersion;
    }

    public ConditionType getConditionType() {
        return conditionType;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setPreTaskCode(long preTaskCode) {
        this.preTaskCode = preTaskCode;
    }

    public void setPreTaskVersion(int preTaskVersion) {
        this.preTaskVersion = preTaskVersion;
    }

    public void setPostTaskCode(long postTaskCode) {
        this.postTaskCode = postTaskCode;
    }

    public void setPostTaskVersion(int postTaskVersion) {
        this.postTaskVersion = postTaskVersion;
    }

    public void setConditionType(ConditionType conditionType) {
        this.conditionType = conditionType;
    }
}
