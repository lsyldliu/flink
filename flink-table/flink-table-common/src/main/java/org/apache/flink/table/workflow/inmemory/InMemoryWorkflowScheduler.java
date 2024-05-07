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

package org.apache.flink.table.workflow.inmemory;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.CreateRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflow;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowException;
import org.apache.flink.table.workflow.WorkflowScheduler;

/** An in-memory workflow scheduler plugin for test. */
@PublicEvolving
public class InMemoryWorkflowScheduler implements WorkflowScheduler<InMemoryRefreshHandler> {

    @Override
    public void open() throws WorkflowException {}

    @Override
    public void close() throws WorkflowException {}

    @Override
    public RefreshHandlerSerializer getRefreshHandlerSerializer() {
        return null;
    }

    @Override
    public InMemoryRefreshHandler createRefreshWorkflow(CreateRefreshWorkflow createRefreshWorkflow)
            throws WorkflowException {
        if (createRefreshWorkflow instanceof CreatePeriodicRefreshWorkflow) {

        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported create refresh workflow %s", createRefreshWorkflow));
        }
        return null;
    }

    @Override
    public void modifyRefreshWorkflow(
            ModifyRefreshWorkflow<InMemoryRefreshHandler> modifyRefreshWorkflow)
            throws WorkflowException {
        InMemoryRefreshHandler inMemoryRefreshHandler = modifyRefreshWorkflow.getRefreshHandler();
        if (modifyRefreshWorkflow instanceof SuspendRefreshWorkflow) {

        } else if (modifyRefreshWorkflow instanceof ResumeRefreshWorkflow) {

        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported modify refresh workflow %s", modifyRefreshWorkflow));
        }
    }

    @Override
    public void deleteRefreshWorkflow(
            DeleteRefreshWorkflow<InMemoryRefreshHandler> deleteRefreshWorkflow)
            throws WorkflowException {
        InMemoryRefreshHandler inMemoryRefreshHandler = deleteRefreshWorkflow.getRefreshHandler();
    }
}
