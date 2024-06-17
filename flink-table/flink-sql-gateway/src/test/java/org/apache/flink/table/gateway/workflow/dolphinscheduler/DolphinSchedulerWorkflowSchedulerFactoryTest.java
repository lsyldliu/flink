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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil.createWorkflowScheduler;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for dolphinscheduler workflow scheduler plugin. */
public class DolphinSchedulerWorkflowSchedulerFactoryTest {

    @Test
    void testCreateWorkflowScheduler() throws Exception {
        final Map<String, String> options = getDefaultConfig();
        WorkflowScheduler<?> actual =
                createWorkflowScheduler(
                        Configuration.fromMap(options),
                        Thread.currentThread().getContextClassLoader());

        assertThat(actual).isInstanceOf(DolphinSchedulerWorkflowScheduler.class);
        actual.open();
    }

    private Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("workflow-scheduler.type", "dolphinscheduler");
        config.put("workflow-scheduler.dolphinscheduler.endpoint", "127.0.0.1");
        config.put("workflow-scheduler.dolphinscheduler.port", "5173");
        config.put("workflow-scheduler.dolphinscheduler.user-name", "admin");
        config.put("workflow-scheduler.dolphinscheduler.password", "dolphinscheduler123");
        config.put("workflow-scheduler.dolphinscheduler.project-name", "test");
        return config;
    }
}
