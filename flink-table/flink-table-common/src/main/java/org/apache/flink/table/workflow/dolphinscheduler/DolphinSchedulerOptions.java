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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Options to configure {@link DolphinSchedulerWorkflowScheduler}. */
@PublicEvolving
public class DolphinSchedulerOptions {

    /** The user name used to connect to dolphinscheduler service. */
    public static final ConfigOption<String> USER_NAME =
            key("user-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The user name used to connect to dolphinscheduler service.");

    /** The password used to connect to dolphinscheduler service. */
    public static final ConfigOption<String> PASSWORD =
            key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password used to connect to dolphinscheduler service.");

    /** The dolphinscheduler service url. */
    public static final ConfigOption<String> ENDPOINT =
            key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The dolphinscheduler service url.");

    public static final ConfigOption<String> PROJECT_NAME =
            key("project-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The project name that workflow will be created in.");
}
