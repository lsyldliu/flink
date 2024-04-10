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

package org.apache.flink.table.workflow;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.dynamic.RefreshHandler;
import org.apache.flink.table.catalog.dynamic.RefreshHandlerSerializer;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * The workflow scheduler interface that used to create/suspend/resume/drop refresh workflow of
 * dynamic table.
 */
@PublicEvolving
public interface WorkflowScheduler {

    /**
     * Create a {@link RefreshHandlerSerializer} to serialize and deserialize {@link
     * RefreshHandler}.
     */
    RefreshHandlerSerializer createSerializer();

    /**
     * Create a refresh workflow in scheduler corresponding to the dynamic table. Return a {@link
     * RefreshHandler} which can locate the refresh workflow detail information.
     *
     * @param dynamicTableIdentifier The dynamic table identifier to be registered.
     * @param triggerUrl The url of scheduler used to trigger a refresh workflow execution.
     * @param executionConf The flink job execution conf.
     * @return The detail refresh workflow information.
     */
    RefreshHandler createRefreshWorkflow(
            ObjectIdentifier dynamicTableIdentifier,
            String refreshStatement,
            String workflowNamePrefix,
            @Nullable String refreshCron,
            boolean isPeriodic,
            String triggerUrl,
            Map<String, String> executionConf);

    /**
     * Suspend the refresh workflow in scheduler.
     *
     * @param refreshHandler The detail refresh workflow information.
     */
    boolean suspendRefreshWorkflow(RefreshHandler refreshHandler);

    /**
     * Resume the refresh workflow in scheduler with new execution conf.
     *
     * @param refreshHandler The detail refresh workflow information
     * @param updatedExecutionConf The updated flink job execution conf.
     */
    boolean resumeRefreshWorkflow(
            RefreshHandler refreshHandler, Map<String, String> updatedExecutionConf);

    /**
     * Delete the refresh workflow in scheduler.
     *
     * @param refreshHandler The detail refresh workflow information.
     */
    boolean deleteRefreshWorkflow(RefreshHandler refreshHandler);
}
