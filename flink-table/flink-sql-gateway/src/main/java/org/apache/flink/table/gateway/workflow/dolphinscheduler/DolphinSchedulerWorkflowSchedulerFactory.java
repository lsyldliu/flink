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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.factories.WorkflowSchedulerFactory;
import org.apache.flink.table.factories.WorkflowSchedulerFactoryUtil;
import org.apache.flink.table.workflow.WorkflowScheduler;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/** The {@link WorkflowSchedulerFactory} to create the {@link DolphinSchedulerWorkflowScheduler}. */
@PublicEvolving
public class DolphinSchedulerWorkflowSchedulerFactory implements WorkflowSchedulerFactory {

    public static final String IDENTIFIER = "dolphinscheduler";

    public static final ConfigOption<String> ENDPOINT =
            ConfigOptions.key("endpoint").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port").intType().noDefaultValue();
    public static final ConfigOption<String> USER_NAME =
            ConfigOptions.key("user-name").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();
    public static final ConfigOption<String> PROJECT_NAME =
            ConfigOptions.key("project-name").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ENDPOINT);
        options.add(PORT);
        options.add(USER_NAME);
        options.add(PASSWORD);
        options.add(PROJECT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public WorkflowScheduler<?> createWorkflowScheduler(Context context) {
        WorkflowSchedulerFactoryUtil.WorkflowSchedulerFactoryHelper helper =
                WorkflowSchedulerFactoryUtil.createWorkflowSchedulerFactoryHelper(this, context);
        helper.validate();

        return new DolphinSchedulerWorkflowScheduler(
                helper.getOptions().get(ENDPOINT),
                helper.getOptions().get(PORT),
                helper.getOptions().get(USER_NAME),
                helper.getOptions().get(PASSWORD),
                helper.getOptions().get(PROJECT_NAME));
    }
}
