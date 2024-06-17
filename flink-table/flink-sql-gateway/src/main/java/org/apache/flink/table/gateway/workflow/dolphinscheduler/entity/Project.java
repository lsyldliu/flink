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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

/** Project info. */
public class Project {

    /** id */
    private final Integer id;

    /** user id */
    private final Integer userId;

    /** user name */
    private final String userName;

    /** project code */
    private final long code;

    /** project name */
    private final String name;

    /** project description */
    private final String description;

    /** create time */
    private final Date createTime;

    /** update time */
    private final Date updateTime;

    /** permission */
    private final int perm;

    /** process define count */
    private final int defCount;

    /** process instance running count */
    private final int instRunningCount;

    @JsonCreator
    public Project(
            @JsonProperty("id") Integer id,
            @JsonProperty("userId") Integer userId,
            @JsonProperty("userName") String userName,
            @JsonProperty("code") long code,
            @JsonProperty("name") String name,
            @JsonProperty("description") String description,
            @JsonProperty("createTime") Date createTime,
            @JsonProperty("updateTime") Date updateTime,
            @JsonProperty("perm") int perm,
            @JsonProperty("defCount") int defCount,
            @JsonProperty("instRunningCount") int instRunningCount) {
        this.id = id;
        this.userId = userId;
        this.userName = userName;
        this.code = code;
        this.name = name;
        this.description = description;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.perm = perm;
        this.defCount = defCount;
        this.instRunningCount = instRunningCount;
    }

    public Integer getId() {
        return id;
    }

    public Integer getUserId() {
        return userId;
    }

    public String getUserName() {
        return userName;
    }

    public long getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public int getPerm() {
        return perm;
    }

    public int getDefCount() {
        return defCount;
    }

    public int getInstRunningCount() {
        return instRunningCount;
    }
}
