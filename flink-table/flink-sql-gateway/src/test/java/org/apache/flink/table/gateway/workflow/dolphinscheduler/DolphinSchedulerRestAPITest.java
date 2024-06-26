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
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.HttpResponse;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.HttpResponseBody;
import org.apache.flink.table.gateway.workflow.dolphinscheduler.http.RequestClient;
import org.apache.flink.table.workflow.WorkflowException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/** Test for dolphinscheduler rest api. */
public class DolphinSchedulerRestAPITest {

    private static final RequestClient requestClient =
            new RequestClient("http://127.0.0.1:5173/dolphinscheduler");

    private static String sessionId;
    private static Map<String, String> headers = new HashMap<>();
    private static long projectCode = 111969609116064L;

    @BeforeAll
    static void setUp() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("userName", "admin");
        params.put("userPassword", "dolphinscheduler123");

        String loginUrl = "/login";
        HttpResponse httpResponse = requestClient.post(loginUrl, new HashMap<>(), params);

        sessionId = (String) getValueResponseMap(httpResponse.getBody(), Constants.SESSION_ID_KEY);
        headers.put(Constants.SESSION_ID_KEY, sessionId);
    }

    @Test
    void testLoginAPI() throws Exception {
        // create project url
        String createProjectUrl = "/projects";

        /*        Map<String, Object> createProjectParams = new HashMap<>();
        createProjectParams.put("projectName", "test1");

        HttpResponse createProjectResponse =
                requestClient.post(createProjectUrl, projectHeaders, createProjectParams);

        // LinkedHashMap
        System.out.println(getValueResponseMap(createProjectResponse, "code"));*/

        String projectName = "test";
        // query project
        Map<String, Object> queryProjectParams = new HashMap<>();
        queryProjectParams.put("searchVal", projectName);
        queryProjectParams.put("pageSize", 10000);
        queryProjectParams.put("pageNo", 1);

        HttpResponseBody queryProjectRespBody =
                requestClient.get(createProjectUrl, headers, queryProjectParams).getBody();

        List projectList = (List) getValueResponseMap(queryProjectRespBody, "totalList");
        List<Project> projects = JSONUtils.convertToEntityList(projectList, Project.class);
        Optional<Project> project =
                projects.stream().filter(p -> p.getName().equals(projectName)).findAny();
        if (!project.isPresent()) {
            throw new WorkflowException(
                    String.format("Project %s is not found in dolphinscheduler.", projectName));
        }
        System.out.println(project.get().getCode());
    }

    @Test
    void createMaterializedTableWorkflow2() throws Exception {
        // 1. generate task code
        String generateTaskCodeUrl =
                String.format("/projects/%s/task-definition/gen-task-codes", projectCode);
        Map<String, Object> taskCodeParams = new HashMap<>();
        taskCodeParams.put("genNum", 1);

        List<Long> taskCodes =
                (ArrayList)
                        requestClient
                                .get(generateTaskCodeUrl, headers, taskCodeParams)
                                .getBody()
                                .getData();
        long taskCode = taskCodes.get(0);
        // 2. create workflow
        String createWorkflowUrl = String.format("/projects/%s/process-definition", projectCode);
        // Not required: locations params
        // 2.1 taskRelationJson
        TaskRelation taskRelation = new TaskRelation();
        taskRelation.setPreTaskCode(0);
        taskRelation.setPreTaskVersion(0);
        taskRelation.setPostTaskCode(taskCode);
        taskRelation.setPostTaskVersion(0);
        taskRelation.setConditionType(ConditionType.NONE);
        // 2.2 Flink parameter
        MaterializedTableParameters parameters = new MaterializedTableParameters();
        parameters.setIdentifier("`a`.`b`.`c`");
        parameters.setGatewayEndpoint("127.0.0.1:8083");
        parameters.setStatementDescription("ALTER MATERIALIZED TABLE `a`.`b`.`c` REFRESH");
        parameters.setPeriodic(true);
        // 2.3 taskDefinitionJson
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setCode(taskCode);
        taskDefinition.setName("refresh_materialized_table_`a`.`b`.`c`2");
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
        createWorkflowParams.put("name", "api_create_materialized_table_workflow2");
        createWorkflowParams.put(
                "taskDefinitionJson", JSONUtils.toJsonString(Arrays.asList(taskDefinition)));
        createWorkflowParams.put(
                "taskRelationJson", JSONUtils.toJsonString(Arrays.asList(taskRelation)));

        HttpResponseBody createWorkflowRespBody =
                requestClient.post(createWorkflowUrl, headers, createWorkflowParams).getBody();
        if (!createWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            createWorkflowRespBody.getMsg()));
        }
        Object workflowCode = getValueResponseMap(createWorkflowRespBody, "code");
        // 3. online workflow
        String onlineWorkflowUrl =
                String.format(
                        "/projects/%s/process-definition/%s/release", projectCode, workflowCode);
        Map<String, Object> onlineWorkflowParams = new HashMap<>();
        onlineWorkflowParams.put("releaseState", ReleaseState.ONLINE);

        HttpResponseBody onlineWorkflowRespBody =
                requestClient.post(onlineWorkflowUrl, headers, onlineWorkflowParams).getBody();
        if (!onlineWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineWorkflowRespBody.getMsg()));
        }

        // 4. create schedule
        String createScheduleUrl = String.format("/projects/%s/schedules", projectCode);
        LocalDate today = LocalDate.now();
        ZonedDateTime startOfDay = today.atStartOfDay(ZoneId.systemDefault());
        Date startOfDayDate = Date.from(startOfDay.toInstant());
        // 100年后的同一日期
        LocalDate in100Years = today.plusYears(100);
        ZonedDateTime zonedDateTimeIn100Years = in100Years.atStartOfDay(ZoneId.systemDefault());
        Date in100YearsDate = Date.from(zonedDateTimeIn100Years.toInstant());

        String cronExpression = "0 0/30 * * * ? *";
        ScheduleParam scheduleParam =
                new ScheduleParam(
                        startOfDayDate,
                        in100YearsDate,
                        TimeZone.getDefault().getID(),
                        cronExpression);

        Map<String, Object> createScheduleParams = new HashMap<>();
        createScheduleParams.put("schedule", JSONUtils.toJsonString(scheduleParam));
        createScheduleParams.put("processDefinitionCode", workflowCode);

        HttpResponseBody createScheduleRespBody =
                requestClient.post(createScheduleUrl, headers, createScheduleParams).getBody();
        if (!createScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineWorkflowRespBody.getMsg()));
        }

        Object scheduleId = getValueResponseMap(createScheduleRespBody, "id");
        // 5. online schedule
        String onlineScheduleUrl =
                String.format("/projects/%s/schedules/%s/online", projectCode, scheduleId);
        HttpResponseBody onlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!onlineScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineScheduleRespBody.getMsg()));
        }
    }

    @Test
    void createMaterializedTableWorkflow() throws Exception {
        // 1. generate task code
        String generateTaskCodeUrl =
                String.format("/projects/%s/task-definition/gen-task-codes", projectCode);
        Map<String, Object> taskCodeParams = new HashMap<>();
        taskCodeParams.put("genNum", 1);

        List<Long> taskCodes =
                (ArrayList)
                        requestClient
                                .get(generateTaskCodeUrl, headers, taskCodeParams)
                                .getBody()
                                .getData();
        long taskCode = taskCodes.get(0);
        // 2. create workflow
        String createWorkflowUrl = String.format("/projects/%s/process-definition", projectCode);
        // Not required: locations params
        // 2.1 taskRelationJson
        TaskRelation taskRelation = new TaskRelation();
        taskRelation.setPreTaskCode(0);
        taskRelation.setPreTaskVersion(0);
        taskRelation.setPostTaskCode(taskCode);
        taskRelation.setPostTaskVersion(0);
        taskRelation.setConditionType(ConditionType.NONE);
        // 2.2 Flink parameter
        MaterializedTableParameters parameters = new MaterializedTableParameters();
        parameters.setIdentifier("`a`.`b`.`c`");
        parameters.setGatewayEndpoint("127.0.0.1:8083");
        parameters.setStatementDescription("ALTER MATERIALIZED TABLE `a`.`b`.`c` REFRESH");
        parameters.setPeriodic(true);
        // 2.3 taskDefinitionJson
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setCode(taskCode);
        taskDefinition.setName("refresh_materialized_table_`a`.`b`.`c`");
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
        createWorkflowParams.put("name", "api_create_materialized_table_workflow");
        createWorkflowParams.put(
                "taskDefinitionJson", JSONUtils.toJsonString(Arrays.asList(taskDefinition)));
        createWorkflowParams.put(
                "taskRelationJson", JSONUtils.toJsonString(Arrays.asList(taskRelation)));

        HttpResponseBody createWorkflowRespBody =
                requestClient.post(createWorkflowUrl, headers, createWorkflowParams).getBody();
        if (!createWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            createWorkflowRespBody.getMsg()));
        }
        Object workflowCode = getValueResponseMap(createWorkflowRespBody, "code");
        // 3. online workflow
        String onlineWorkflowUrl =
                String.format(
                        "/projects/%s/process-definition/%s/release", projectCode, workflowCode);
        Map<String, Object> onlineWorkflowParams = new HashMap<>();
        onlineWorkflowParams.put("releaseState", ReleaseState.ONLINE);

        HttpResponseBody onlineWorkflowRespBody =
                requestClient.post(onlineWorkflowUrl, headers, onlineWorkflowParams).getBody();
        if (!onlineWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineWorkflowRespBody.getMsg()));
        }

        // 4. create schedule
        String createScheduleUrl = String.format("/projects/%s/schedules", projectCode);
        LocalDate today = LocalDate.now();
        ZonedDateTime startOfDay = today.atStartOfDay(ZoneId.systemDefault());
        Date startOfDayDate = Date.from(startOfDay.toInstant());
        // 100年后的同一日期
        LocalDate in100Years = today.plusYears(100);
        ZonedDateTime zonedDateTimeIn100Years = in100Years.atStartOfDay(ZoneId.systemDefault());
        Date in100YearsDate = Date.from(zonedDateTimeIn100Years.toInstant());

        String cronExpression = "0 0/30 * * * ? *";
        ScheduleParam scheduleParam =
                new ScheduleParam(
                        startOfDayDate,
                        in100YearsDate,
                        TimeZone.getDefault().getID(),
                        cronExpression);

        Map<String, Object> createScheduleParams = new HashMap<>();
        createScheduleParams.put("schedule", JSONUtils.toJsonString(scheduleParam));
        createScheduleParams.put("processDefinitionCode", workflowCode);

        HttpResponseBody createScheduleRespBody =
                requestClient.post(createScheduleUrl, headers, createScheduleParams).getBody();
        if (!createScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineWorkflowRespBody.getMsg()));
        }

        Object scheduleId = getValueResponseMap(createScheduleRespBody, "id");
        // 5. online schedule
        String onlineScheduleUrl =
                String.format("/projects/%s/schedules/%s/online", projectCode, scheduleId);
        HttpResponseBody onlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!onlineScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineScheduleRespBody.getMsg()));
        }
    }

    @Test
    void suspendMaterializedTableWorkflow() throws Exception {
        String onlineScheduleUrl =
                String.format("/projects/%s/schedules/%s/offline", projectCode, 1);
        HttpResponseBody offlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!offlineScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            offlineScheduleRespBody.getMsg()));
        }
    }

    @Test
    void resumeMaterializedTableWorkflowWithoutOptions() throws Exception {
        String onlineScheduleUrl =
                String.format("/projects/%s/schedules/%s/online", projectCode, 1);
        HttpResponseBody onlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!onlineScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Create workflow occur exception, root cause: %s.",
                            onlineScheduleRespBody.getMsg()));
        }
    }

    @Test
    void resumeMaterializedTableWorkflowWithOptions() throws Exception {
        // processCode: 111869964373376L

        // 1. get task info
        String taskInfoUrl =
                String.format("/projects/%s/task-definition/%s", projectCode, 111983532362144L);
        HttpResponseBody taskInfoRespBody =
                requestClient.get(taskInfoUrl, headers, new HashMap<>()).getBody();
        if (!taskInfoRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Get task occur exception, root cause: %s.",
                            taskInfoRespBody.getMsg()));
        }

        TaskDefinition taskDefinition =
                JSONUtils.convertToEntity(
                        (LinkedHashMap<String, String>) taskInfoRespBody.getData(),
                        TaskDefinition.class);

        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("a", "b");
        dynamicOptions.put("c", "d");
        dynamicOptions.put("e", "f");
        taskDefinition.getTaskParams().setDynamicOptions(JSONUtils.toJsonString(dynamicOptions));

        // 2. offline workflow
        String offlineWorkflowUrl =
                String.format(
                        "/projects/%s/process-definition/%s/release",
                        projectCode, 111983532554656L);
        Map<String, Object> offlineWorkflowParams = new HashMap<>();
        offlineWorkflowParams.put("releaseState", ReleaseState.OFFLINE);

        HttpResponseBody offlineWorkflowRespBody =
                requestClient.post(offlineWorkflowUrl, headers, offlineWorkflowParams).getBody();
        if (!offlineWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Resume workflow occur exception, root cause: %s.",
                            offlineWorkflowRespBody.getMsg()));
        }

        // 3. update task
        String updateTaskUrl =
                String.format("/projects/%s/task-definition/%s", projectCode, 111869964373376L);
        Map<String, Object> updateTaskParams = new HashMap<>();
        updateTaskParams.put("taskDefinitionJsonObj", JSONUtils.toJsonString(taskDefinition));

        HttpResponseBody updateTaskRespBody =
                requestClient.put(updateTaskUrl, headers, updateTaskParams).getBody();
        if (!updateTaskRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Update task occur exception, root cause: %s.",
                            updateTaskRespBody.getMsg()));
        }

        // 4. online workflow
        String onlineWorkflowUrl =
                String.format(
                        "/projects/%s/process-definition/%s/release",
                        projectCode, 111869964719488L);
        Map<String, Object> onlineWorkflowParams = new HashMap<>();
        onlineWorkflowParams.put("releaseState", ReleaseState.ONLINE);

        HttpResponseBody onlineWorkflowRespBody =
                requestClient.post(onlineWorkflowUrl, headers, onlineWorkflowParams).getBody();
        if (!onlineWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Resume workflow occur exception, root cause: %s.",
                            onlineWorkflowRespBody.getMsg()));
        }

        // 5. online schedule
        String onlineScheduleUrl =
                String.format("/projects/%s/schedules/%s/online", projectCode, 1);
        HttpResponseBody onlineScheduleRespBody =
                requestClient.post(onlineScheduleUrl, headers, new HashMap<>()).getBody();
        if (!onlineScheduleRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Resume workflow occur exception, root cause: %s.",
                            onlineScheduleRespBody.getMsg()));
        }
    }

    @Test
    void testDeleteMaterializedTableWorkflow() throws Exception {
        // 1. offline workflow
        String offlineWorkflowUrl =
                String.format(
                        "/projects/%s/process-definition/%s/release",
                        projectCode, 111869964719488L);
        Map<String, Object> offlineWorkflowParams = new HashMap<>();
        offlineWorkflowParams.put("releaseState", ReleaseState.OFFLINE);

        HttpResponseBody offlineWorkflowRespBody =
                requestClient.post(offlineWorkflowUrl, headers, offlineWorkflowParams).getBody();
        if (!offlineWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Offline workflow occur exception, root cause: %s.",
                            offlineWorkflowRespBody.getMsg()));
        }

        String deleteWorkflowUrl =
                String.format("/projects/%s/process-definition/%s", projectCode, 111869964719488L);
        HttpResponseBody deleteWorkflowRespBody =
                requestClient.delete(deleteWorkflowUrl, headers, new HashMap<>()).getBody();
        if (!deleteWorkflowRespBody.getSuccess()) {
            throw new WorkflowException(
                    String.format(
                            "Delete workflow occur exception, root cause: %s.",
                            offlineWorkflowRespBody.getMsg()));
        }
    }

    private static Object getValueResponseMap(HttpResponseBody httpResponseBody, String key) {
        Map<String, Object> resultMap = (LinkedHashMap) httpResponseBody.getData();
        return resultMap.get(key);
    }
}
