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

import java.util.Date;
import java.util.Objects;

/** Task Definition info. */
public class TaskDefinition {

    /** id */
    private Integer id;

    /** code */
    private long code;

    /** name */
    private String name;

    /** version */
    private int version;

    /** description */
    private String description;

    /** project code */
    private long projectCode;

    /** task user id */
    private int userId;

    /** task type */
    private String taskType;

    /** user defined parameters */
    private MaterializedTableParameters taskParams;

    /** task is valid: yes/no */
    private Flag flag;

    /** task is cache: yes/no */
    private Flag isCache;

    /** task priority */
    private Priority taskPriority;

    /** user name */
    private String userName;

    /** project name */
    private String projectName;

    /** worker group */
    private String workerGroup;

    /** environment code */
    private long environmentCode;

    /** fail retry times */
    private int failRetryTimes;

    /** fail retry interval */
    private int failRetryInterval;

    /** timeout flag */
    private TimeoutFlag timeoutFlag;

    /** timeout notify strategy */
    private TaskTimeoutStrategy timeoutNotifyStrategy;

    /** task warning time out. unit: minute */
    private int timeout;

    /** delay execution time. */
    private int delayTime;

    /** resource ids we do */
    @Deprecated private String resourceIds;

    /** create time */
    private Date createTime;

    /** update time */
    private Date updateTime;

    /** modify user name */
    private String modifyBy;

    /** task group id */
    private int taskGroupId;
    /** task group id */
    private int taskGroupPriority;

    /** cpu quota */
    private Integer cpuQuota;

    /** max memory */
    private Integer memoryMax;

    /** task execute type */
    private TaskExecuteType taskExecuteType;

    public TaskDefinition() {}

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public long getCode() {
        return code;
    }

    public void setCode(long code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getProjectCode() {
        return projectCode;
    }

    public void setProjectCode(long projectCode) {
        this.projectCode = projectCode;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public MaterializedTableParameters getTaskParams() {
        return taskParams;
    }

    public void setTaskParams(MaterializedTableParameters taskParams) {
        this.taskParams = taskParams;
    }

    public Flag getFlag() {
        return flag;
    }

    public void setFlag(Flag flag) {
        this.flag = flag;
    }

    public Flag getIsCache() {
        return isCache;
    }

    public void setIsCache(Flag isCache) {
        this.isCache = isCache;
    }

    public Priority getTaskPriority() {
        return taskPriority;
    }

    public void setTaskPriority(Priority taskPriority) {
        this.taskPriority = taskPriority;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getWorkerGroup() {
        return workerGroup;
    }

    public void setWorkerGroup(String workerGroup) {
        this.workerGroup = workerGroup;
    }

    public long getEnvironmentCode() {
        return environmentCode;
    }

    public void setEnvironmentCode(long environmentCode) {
        this.environmentCode = environmentCode;
    }

    public int getFailRetryTimes() {
        return failRetryTimes;
    }

    public void setFailRetryTimes(int failRetryTimes) {
        this.failRetryTimes = failRetryTimes;
    }

    public int getFailRetryInterval() {
        return failRetryInterval;
    }

    public void setFailRetryInterval(int failRetryInterval) {
        this.failRetryInterval = failRetryInterval;
    }

    public TimeoutFlag getTimeoutFlag() {
        return timeoutFlag;
    }

    public void setTimeoutFlag(TimeoutFlag timeoutFlag) {
        this.timeoutFlag = timeoutFlag;
    }

    public TaskTimeoutStrategy getTimeoutNotifyStrategy() {
        return timeoutNotifyStrategy;
    }

    public void setTimeoutNotifyStrategy(TaskTimeoutStrategy timeoutNotifyStrategy) {
        this.timeoutNotifyStrategy = timeoutNotifyStrategy;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    public String getResourceIds() {
        return resourceIds;
    }

    public void setResourceIds(String resourceIds) {
        this.resourceIds = resourceIds;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getModifyBy() {
        return modifyBy;
    }

    public void setModifyBy(String modifyBy) {
        this.modifyBy = modifyBy;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public void setTaskGroupId(int taskGroupId) {
        this.taskGroupId = taskGroupId;
    }

    public int getTaskGroupPriority() {
        return taskGroupPriority;
    }

    public void setTaskGroupPriority(int taskGroupPriority) {
        this.taskGroupPriority = taskGroupPriority;
    }

    public Integer getCpuQuota() {
        return cpuQuota;
    }

    public void setCpuQuota(Integer cpuQuota) {
        this.cpuQuota = cpuQuota;
    }

    public Integer getMemoryMax() {
        return memoryMax;
    }

    public void setMemoryMax(Integer memoryMax) {
        this.memoryMax = memoryMax;
    }

    public TaskExecuteType getTaskExecuteType() {
        return taskExecuteType;
    }

    public void setTaskExecuteType(TaskExecuteType taskExecuteType) {
        this.taskExecuteType = taskExecuteType;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        TaskDefinition that = (TaskDefinition) o;
        return failRetryTimes == that.failRetryTimes
                && failRetryInterval == that.failRetryInterval
                && timeout == that.timeout
                && delayTime == that.delayTime
                && Objects.equals(name, that.name)
                && Objects.equals(description, that.description)
                && Objects.equals(taskType, that.taskType)
                && Objects.equals(taskParams, that.taskParams)
                && flag == that.flag
                && isCache == that.isCache
                && taskPriority == that.taskPriority
                && Objects.equals(workerGroup, that.workerGroup)
                && timeoutFlag == that.timeoutFlag
                && timeoutNotifyStrategy == that.timeoutNotifyStrategy
                && Objects.equals(cpuQuota, that.cpuQuota)
                && Objects.equals(memoryMax, that.memoryMax)
                && Objects.equals(taskExecuteType, that.taskExecuteType);
    }
}
