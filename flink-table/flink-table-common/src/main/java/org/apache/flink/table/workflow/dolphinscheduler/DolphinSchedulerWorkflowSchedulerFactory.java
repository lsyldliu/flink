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

package org.apache.flink.table.workflow.dolphinscheduler;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.workflow.WorkflowScheduler;
import org.apache.flink.table.workflow.WorkflowSchedulerFactory;
import org.apache.flink.table.workflow.WorkflowSchedulerFactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.workflow.dolphinscheduler.DolphinSchedulerOptions.ENDPOINT;
import static org.apache.flink.table.workflow.dolphinscheduler.DolphinSchedulerOptions.PASSWORD;
import static org.apache.flink.table.workflow.dolphinscheduler.DolphinSchedulerOptions.PROJECT_NAME;
import static org.apache.flink.table.workflow.dolphinscheduler.DolphinSchedulerOptions.USER_NAME;

/** DolphinScheduler workflow scheduler factory implementation. */
public class DolphinSchedulerWorkflowSchedulerFactory implements WorkflowSchedulerFactory {

    public static final String IDENTIFIER = "dolphinscheduler";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public WorkflowScheduler createWorkflowScheduler(Context context) {
        WorkflowSchedulerFactoryUtil.WorkflowSchedulerFactoryHelper helper =
                WorkflowSchedulerFactoryUtil.createWorkflowSchedulerFactoryHelper(this, context);
        // validate required options
        helper.validate();
        // create workflow scheduler
        Configuration config = Configuration.fromMap(context.getWorkflowSchedulerOptions());
        String userName = config.get(USER_NAME);
        String password = config.get(PASSWORD);
        String endpoint = config.get(ENDPOINT);
        String projectName = config.get(PROJECT_NAME);
        return new DolphinSchedulerWorkflowScheduler(userName, password, endpoint, projectName);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USER_NAME);
        options.add(PASSWORD);
        options.add(ENDPOINT);
        options.add(PROJECT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
