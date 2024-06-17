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

package org.apache.flink.table.gateway.workflow.dolphinscheduler;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.gateway.workflow.EmbeddedWorkflowScheduler;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.ConditionType;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.Flag;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.MaterializedTableParameters;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.Priority;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.Project;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.ReleaseState;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.ScheduleParam;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.TaskDefinition;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.TaskExecuteType;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.TaskRelation;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.TaskTimeoutStrategy;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.entity.TimeoutFlag;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.Constants;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.HttpResponseBody;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.RequestClient;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * A workflow scheduler plugin implementation for dolphin scheduler. It is used to create, modify
 * refresh workflow for materialized table.
 */
@PublicEvolving
public class DolphinSchedulerWorkflowScheduler
        implements WorkflowScheduler<DolphinSchedulerRefreshHandler> {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedWorkflowScheduler.class);

    private static final String LOGIN_URL = "/login";
    private static final String PROJECT_URL = "/projects";
    private static final String GENERATE_TASK_CODE_URL =
            "/projects/%s/task-definition/gen-task-codes";
    private static final String TASK_URL = "/projects/%s/task-definition/%s";
    private static final String WORKFLOW_URL = "/projects/%s/process-definition";
    private static final String DELETE_WORKFLOW_URL = "/projects/%s/process-definition/%s";
    private static final String WORKFLOW_RELEASE_URL = "/projects/%s/process-definition/%s/release";
    private static final String SCHEDULE_URL = "/projects/%s/schedules";
    private static final String SCHEDULE_ONLINE_URL = "/projects/%s/schedules/%s/online";
    private static final String SCHEDULE_OFFLINE_URL = "/projects/%s/schedules/%s/offline";

    private final String endpoint;
    private final int port;
    private final String userName;
    private final String password;
    private final String projectName;

    private RequestClient requestClient;

    private Map<String, String> headers = new HashMap<>();

    public DolphinSchedulerWorkflowScheduler(
            String endpoint, int port, String userName, String password, String projectName) {
        this.endpoint = endpoint;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.projectName = projectName;
    }

    @Override
    public void open() throws WorkflowException {
        this.requestClient =
                new RequestClient(
                        String.format(Constants.DOLPHINSCHEDULER_API_URL, endpoint, port));

        Map<String, Object> params = new HashMap<>();
        params.put("userName", userName);
        params.put("userPassword", password);

        // login
        HttpResponseBody loginRespBody =
                requestClient.post(LOGIN_URL, new HashMap<>(), params).getBody();
        if (!loginRespBody.getSuccess()) {
            LOG.error(
                    "Login dolphinscheduler occur exception using url: {}, userName: {}, password: {}, root case: {}.",
                    String.format(Constants.DOLPHINSCHEDULER_API_URL, endpoint, port),
                    userName,
                    password,
                    loginRespBody.getMsg());
            throw new WorkflowException(
                    String.format(
                            "Login dolphinscheduler occur exception using url: %s, userName: %s, password: %s, root case: %s.",
                            String.format(Constants.DOLPHINSCHEDULER_API_URL, endpoint, port),
                            userName,
                            password,
                            loginRespBody.getMsg()));
        }

        headers.put(
                Constants.SESSION_ID_KEY,
                (String) getValueResponseMap(loginRespBody, Constants.SESSION_ID_KEY));
    }

    @Override
    public void close() throws WorkflowException {
        headers.clear();
    }

    @Override
    public RefreshHandlerSerializer<DolphinSchedulerRefreshHandler> getRefreshHandlerSerializer() {
        return DolphinSchedulerRefreshHandlerSerializer.INSTANCE;
    }

    @Override
    public DolphinSchedulerRefreshHandler createRefreshWorkflow(
            CreateRefreshWorkflow createRefreshWorkflow) throws WorkflowException {
        if (createRefreshWorkflow instanceof CreatePeriodicRefreshWorkflow) {
            CreatePeriodicRefreshWorkflow createPeriodicRefreshWorkflow =
                    (CreatePeriodicRefreshWorkflow) createRefreshWorkflow;

            // 1. query project
            Map<String, Object> queryProjectParams = new HashMap<>();
            queryProjectParams.put("searchVal", projectName);
            queryProjectParams.put("pageSize", 10000);
            queryProjectParams.put("pageNo", 1);

            HttpResponseBody queryProjectRespBody =
                    requestClient.get(PROJECT_URL, headers, queryProjectParams).getBody();

            List<Object> projectList =
                    (List<Object>) getValueResponseMap(queryProjectRespBody, "totalList");
            List<Project> projects = JSONUtils.convertToEntityList(projectList, Project.class);
            Optional<Project> project =
                    projects.stream().filter(p -> p.getName().equals(projectName)).findAny();
            if (!project.isPresent()) {
                LOG.error("Project {} is not found in dolphinscheduler.", projectName);
                throw new WorkflowException(
                        String.format("Project %s is not found in dolphinscheduler.", projectName));
            }
            long projectCode = project.get().getCode();

            // 2. generate task code
            String generateTaskCodeUrl = String.format(GENERATE_TASK_CODE_URL, projectCode);
            Map<String, Object> taskCodeParams = new HashMap<>();
            taskCodeParams.put("genNum", 1);

            List<Long> taskCodes =
                    (ArrayList)
                            requestClient
                                    .get(generateTaskCodeUrl, headers, taskCodeParams)
                                    .getBody()
                                    .getData();
            long taskCode = taskCodes.get(0);
            // 3. create workflow
            // Not required: locations params
            // 3.1 taskRelationJson
            TaskRelation taskRelation = new TaskRelation();
            taskRelation.setPreTaskCode(0);
            taskRelation.setPreTaskVersion(0);
            taskRelation.setPostTaskCode(taskCode);
            taskRelation.setPostTaskVersion(0);
            taskRelation.setConditionType(ConditionType.NONE);

            // 3.2 Materialized table parameter
            MaterializedTableParameters parameters = new MaterializedTableParameters();
            String materializedTableIdentifier =
                    createPeriodicRefreshWorkflow
                            .getMaterializedTableIdentifier()
                            .asSerializableString();
            parameters.setIdentifier(materializedTableIdentifier);
            parameters.setGatewayEndpoint(createPeriodicRefreshWorkflow.getRestEndpointUrl());
            parameters.setStatementDescription(
                    createPeriodicRefreshWorkflow.getDescriptionStatement());
            parameters.setPeriodic(true);

            // 3.3 taskDefinitionJson
            TaskDefinition taskDefinition = new TaskDefinition();
            taskDefinition.setCode(taskCode);
            taskDefinition.setName(
                    String.format(
                            "materialized_table_%s_refresh_task", materializedTableIdentifier));
            taskDefinition.setTaskParams(parameters);
            taskDefinition.setTaskPriority(Priority.MEDIUM);
            taskDefinition.setWorkerGroup("default");
            taskDefinition.setTaskType("FLINK_MATERIALIZED_TABLE");
            taskDefinition.setTaskExecuteType(TaskExecuteType.BATCH);
            taskDefinition.setDelayTime(0);
            taskDefinition.setDescription("");
            taskDefinition.setEnvironmentCode(-1);
            taskDefinition.setFailRetryTimes(0);
            taskDefinition.setFailRetryInterval(0);
            taskDefinition.setFlag(Flag.YES);
            taskDefinition.setIsCache(Flag.NO);
            taskDefinition.setTimeout(0);
            taskDefinition.setTimeoutFlag(TimeoutFlag.CLOSE);
            taskDefinition.setTimeoutNotifyStrategy(TaskTimeoutStrategy.WARN);
            taskDefinition.setCpuQuota(-1);
            taskDefinition.setMemoryMax(-1);

            Map<String, Object> createWorkflowParams = new HashMap<>();
            String workflowName =
                    String.format(
                            "materialized_table_%s_refresh_workflow", materializedTableIdentifier);
            createWorkflowParams.put("name", workflowName);
            createWorkflowParams.put(
                    "taskDefinitionJson",
                    JSONUtils.toJsonString(Collections.singletonList(taskDefinition)));
            createWorkflowParams.put(
                    "taskRelationJson",
                    JSONUtils.toJsonString(Collections.singletonList(taskRelation)));

            String createWorkflowUrl = String.format(WORKFLOW_URL, projectCode);
            HttpResponseBody createWorkflowRespBody =
                    requestClient.post(createWorkflowUrl, headers, createWorkflowParams).getBody();
            if (!createWorkflowRespBody.getSuccess()) {
                LOG.error(
                        "Create workflow occur exception, root cause: {}.",
                        createWorkflowRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Create workflow occur exception, root cause: %s.",
                                createWorkflowRespBody.getMsg()));
            }
            long workflowCode = (Long) getValueResponseMap(createWorkflowRespBody, "code");
            // 4. online workflow
            String onlineWorkflowUrl =
                    String.format(WORKFLOW_RELEASE_URL, projectCode, workflowCode);
            Map<String, Object> onlineWorkflowParams = new HashMap<>();
            onlineWorkflowParams.put("releaseState", ReleaseState.ONLINE);

            HttpResponseBody onlineWorkflowRespBody =
                    requestClient.post(onlineWorkflowUrl, headers, onlineWorkflowParams).getBody();
            if (!onlineWorkflowRespBody.getSuccess()) {
                LOG.error(
                        "Create workflow occur exception, root cause: {}.",
                        onlineWorkflowRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Create workflow occur exception, root cause: %s.",
                                onlineWorkflowRespBody.getMsg()));
            }

            // 5. create schedule
            String createScheduleUrl = String.format(SCHEDULE_URL, projectCode);
            LocalDate today = LocalDate.now();
            ZonedDateTime startOfDay = today.atStartOfDay(ZoneId.systemDefault());
            Date startOfDayDate = Date.from(startOfDay.toInstant());
            // Same date 100 years later
            LocalDate in100Years = today.plusYears(100);
            ZonedDateTime zonedDateTimeIn100Years = in100Years.atStartOfDay(ZoneId.systemDefault());
            Date in100YearsDate = Date.from(zonedDateTimeIn100Years.toInstant());
            ScheduleParam scheduleParam =
                    new ScheduleParam(
                            startOfDayDate,
                            in100YearsDate,
                            TimeZone.getDefault().getID(),
                            createPeriodicRefreshWorkflow.getCronExpression());

            Map<String, Object> createScheduleParams = new HashMap<>();
            createScheduleParams.put("processDefinitionCode", workflowCode);
            createScheduleParams.put("schedule", JSONUtils.toJsonString(scheduleParam));

            HttpResponseBody createScheduleRespBody =
                    requestClient.post(createScheduleUrl, headers, createScheduleParams).getBody();
            if (!createScheduleRespBody.getSuccess()) {
                LOG.error(
                        "Create workflow occur exception, root cause: {}.",
                        createScheduleRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Create workflow occur exception, root cause: %s.",
                                createScheduleRespBody.getMsg()));
            }
            int scheduleId = (Integer) getValueResponseMap(createScheduleRespBody, "id");

            // 6. online schedule
            String onlineScheduleUrl = String.format(SCHEDULE_ONLINE_URL, projectCode, scheduleId);
            HttpResponseBody onlineScheduleRespBody =
                    requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
            if (!onlineScheduleRespBody.getSuccess()) {
                LOG.error(
                        "Create workflow occur exception, root cause: {}.",
                        onlineScheduleRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Create workflow occur exception, root cause: %s.",
                                onlineScheduleRespBody.getMsg()));
            }

            return new DolphinSchedulerRefreshHandler(
                    projectCode, workflowCode, workflowName, taskCode, scheduleId);
        } else {
            LOG.error(
                    "Unsupported create refresh workflow type {}.",
                    createRefreshWorkflow.getClass().getSimpleName());
            throw new WorkflowException(
                    String.format(
                            "Unsupported create refresh workflow type %s.",
                            createRefreshWorkflow.getClass().getSimpleName()));
        }
    }

    @Override
    public void modifyRefreshWorkflow(
            ModifyRefreshWorkflow<DolphinSchedulerRefreshHandler> modifyRefreshWorkflow)
            throws WorkflowException {
        DolphinSchedulerRefreshHandler refreshHandler = modifyRefreshWorkflow.getRefreshHandler();
        if (modifyRefreshWorkflow instanceof SuspendRefreshWorkflow) {
            suspendWorkflow(refreshHandler);
        } else if (modifyRefreshWorkflow instanceof ResumeRefreshWorkflow) {
            resumeWorkflow(
                    refreshHandler,
                    ((ResumeRefreshWorkflow) modifyRefreshWorkflow).getDynamicOptions());
        } else {
            LOG.error(
                    "Unsupported modify refresh workflow type {}.",
                    modifyRefreshWorkflow.getClass().getSimpleName());
            throw new WorkflowException(
                    String.format(
                            "Unsupported modify refresh workflow type %s.",
                            modifyRefreshWorkflow.getClass().getSimpleName()));
        }
    }

    @Override
    public void deleteRefreshWorkflow(
            DeleteRefreshWorkflow<DolphinSchedulerRefreshHandler> deleteRefreshWorkflow)
            throws WorkflowException {
        long projectCode = deleteRefreshWorkflow.getRefreshHandler().getProjectCode();
        long workflowCode = deleteRefreshWorkflow.getRefreshHandler().getWorkflowCode();

        // 1. offline workflow
        String offlineWorkflowUrl = String.format(WORKFLOW_RELEASE_URL, projectCode, workflowCode);
        Map<String, Object> offlineWorkflowParams = new HashMap<>();
        offlineWorkflowParams.put("releaseState", ReleaseState.OFFLINE);

        HttpResponseBody offlineWorkflowRespBody =
                requestClient.post(offlineWorkflowUrl, headers, offlineWorkflowParams).getBody();
        if (!offlineWorkflowRespBody.getSuccess()) {
            LOG.error("Offline workflow occur exception, root cause: {}.", offlineWorkflowRespBody);
            throw new WorkflowException(
                    String.format(
                            "Offline workflow occur exception, root cause: %s.",
                            offlineWorkflowRespBody.getMsg()));
        }

        // 2. delete workflow
        String deleteWorkflowUrl = String.format(DELETE_WORKFLOW_URL, projectCode, workflowCode);
        HttpResponseBody deleteWorkflowRespBody =
                requestClient.delete(deleteWorkflowUrl, headers, new HashMap<>()).getBody();
        if (!deleteWorkflowRespBody.getSuccess()) {
            LOG.error(
                    "Delete workflow occur exception, root cause: {}.",
                    deleteWorkflowRespBody.getMsg());
            throw new WorkflowException(
                    String.format(
                            "Delete workflow occur exception, root cause: %s.",
                            deleteWorkflowRespBody.getMsg()));
        }
    }

    private void suspendWorkflow(DolphinSchedulerRefreshHandler refreshHandler)
            throws WorkflowException {
        // offline workflow schedule
        String offlineScheduleUrl =
                String.format(
                        SCHEDULE_OFFLINE_URL,
                        refreshHandler.getProjectCode(),
                        refreshHandler.getSchedulerId());
        HttpResponseBody offlineScheduleRespBody =
                requestClient.post(offlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!offlineScheduleRespBody.getSuccess()) {
            LOG.error(
                    "Offline workflow schedule occur exception, root cause: {}.",
                    offlineScheduleRespBody.getMsg());
            throw new WorkflowException(
                    String.format(
                            "Offline workflow schedule occur exception, root cause: %s.",
                            offlineScheduleRespBody.getMsg()));
        }
    }

    private void resumeWorkflow(
            DolphinSchedulerRefreshHandler refreshHandler, Map<String, String> dynamicOptions)
            throws WorkflowException {
        long projectCode = refreshHandler.getProjectCode();
        long workflowCode = refreshHandler.getWorkflowCode();
        long taskCode = refreshHandler.getTaskCode();
        int scheduleId = refreshHandler.getSchedulerId();

        if (dynamicOptions != null && !dynamicOptions.isEmpty()) {
            // 1. get task info
            String taskInfoUrl = String.format(TASK_URL, projectCode, taskCode);
            HttpResponseBody taskInfoRespBody =
                    requestClient.get(taskInfoUrl, headers, new HashMap<>()).getBody();
            if (!taskInfoRespBody.getSuccess()) {
                LOG.error(
                        "Get task occur exception when resume workflow, root cause: {}.",
                        taskInfoRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Get task occur exception when resume workflow, root cause: %s.",
                                taskInfoRespBody.getMsg()));
            }

            TaskDefinition taskDefinition =
                    JSONUtils.convertToEntity(
                            (LinkedHashMap<String, String>) taskInfoRespBody.getData(),
                            TaskDefinition.class);
            taskDefinition
                    .getTaskParams()
                    .setDynamicOptions(JSONUtils.toJsonString(dynamicOptions));

            // 2. offline workflow
            String offlineWorkflowUrl =
                    String.format(WORKFLOW_RELEASE_URL, projectCode, workflowCode);
            Map<String, Object> offlineWorkflowParams = new HashMap<>();
            offlineWorkflowParams.put("releaseState", ReleaseState.OFFLINE);

            HttpResponseBody offlineWorkflowRespBody =
                    requestClient
                            .post(offlineWorkflowUrl, headers, offlineWorkflowParams)
                            .getBody();
            if (!offlineWorkflowRespBody.getSuccess()) {
                LOG.error(
                        "Resume workflow occur exception, root cause: {}.",
                        offlineWorkflowRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Resume workflow occur exception, root cause: %s.",
                                offlineWorkflowRespBody.getMsg()));
            }

            // 3. update task
            String updateTaskUrl = String.format(TASK_URL, projectCode, taskCode);
            Map<String, Object> updateTaskParams = new HashMap<>();
            updateTaskParams.put("taskDefinitionJsonObj", JSONUtils.toJsonString(taskDefinition));

            HttpResponseBody updateTaskRespBody =
                    requestClient.put(updateTaskUrl, headers, updateTaskParams).getBody();
            if (!updateTaskRespBody.getSuccess()) {
                LOG.error(
                        "Update task occur exception when resume workflow, root cause: {}.",
                        updateTaskRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Update task occur exception when resume workflow, root cause: %s.",
                                updateTaskRespBody.getMsg()));
            }

            // 4. online workflow
            String onlineWorkflowUrl =
                    String.format(WORKFLOW_RELEASE_URL, projectCode, workflowCode);
            Map<String, Object> onlineWorkflowParams = new HashMap<>();
            onlineWorkflowParams.put("releaseState", ReleaseState.ONLINE);

            HttpResponseBody onlineWorkflowRespBody =
                    requestClient.post(onlineWorkflowUrl, headers, onlineWorkflowParams).getBody();
            if (!onlineWorkflowRespBody.getSuccess()) {
                LOG.error(
                        "Resume workflow occur exception, root cause: {}.",
                        onlineWorkflowRespBody.getMsg());
                throw new WorkflowException(
                        String.format(
                                "Resume workflow occur exception, root cause: %s.",
                                onlineWorkflowRespBody.getMsg()));
            }
        }

        // 5. online schedule
        String onlineScheduleUrl = String.format(SCHEDULE_ONLINE_URL, projectCode, scheduleId);
        HttpResponseBody onlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!onlineScheduleRespBody.getSuccess()) {
            LOG.error(
                    "Resume workflow schedule occur exception, root cause: {}.",
                    onlineScheduleRespBody.getMsg());
            throw new WorkflowException(
                    String.format(
                            "Resume workflow schedule occur exception, root cause: %s.",
                            onlineScheduleRespBody.getMsg()));
        }
    }

    private static Object getValueResponseMap(HttpResponseBody httpResponseBody, String key) {
        Map<String, Object> resultMap = (LinkedHashMap) httpResponseBody.getData();
        return resultMap.get(key);
    }
}
