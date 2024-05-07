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

package org.apache.flink.table.gateway.service.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.gateway.service.utils.SqlExecutionException;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.DropMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableOperation;
import org.apache.flink.table.refresh.ContinuousRefreshHandler;
import org.apache.flink.table.refresh.ContinuousRefreshHandlerSerializer;
import org.apache.flink.table.refresh.RefreshHandler;
import org.apache.flink.table.refresh.RefreshHandlerSerializer;
import org.apache.flink.table.workflow.CreatePeriodicRefreshWorkflow;
import org.apache.flink.table.workflow.DeleteRefreshWorkflow;
import org.apache.flink.table.workflow.ModifyRefreshWorkflowCronExpr;
import org.apache.flink.table.workflow.ResumeRefreshWorkflow;
import org.apache.flink.table.workflow.SuspendRefreshWorkflow;
import org.apache.flink.table.workflow.WorkflowScheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.configuration.DeploymentOptions.TARGET;
import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.apache.flink.configuration.PipelineOptions.NAME;
import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL;
import static org.apache.flink.table.api.internal.TableResultInternal.TABLE_RESULT_OK;

/** Manager is responsible for execute the {@link MaterializedTableOperation}. */
@Internal
public class MaterializedTableManager {

    private static final Logger LOG = LoggerFactory.getLogger(MaterializedTableManager.class);

    public static ResultFetcher callMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            MaterializedTableOperation op,
            String statement) {
        if (op instanceof CreateMaterializedTableOperation) {
            return callCreateMaterializedTableOperation(
                    operationExecutor, handle, (CreateMaterializedTableOperation) op);
        }
        throw new SqlExecutionException(
                String.format(
                        "Unsupported Operation %s for materialized table.", op.asSummaryString()));
    }

    private static ResultFetcher callCreateMaterializedTableOperation(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        CatalogMaterializedTable materializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        if (CatalogMaterializedTable.RefreshMode.CONTINUOUS == materializedTable.getRefreshMode()) {
            createMaterializedInContinuousMode(
                    operationExecutor, handle, createMaterializedTableOperation);
        } else {
            throw new SqlExecutionException(
                    "Only support create materialized table in continuous refresh mode currently.");
        }
        // Just return ok for unify different refresh job info of continuous and full mode, user
        // should get the refresh job info via desc table.
        return ResultFetcher.fromTableResult(handle, TABLE_RESULT_OK, false);
    }

    private static void createMaterializedInContinuousMode(
            OperationExecutor operationExecutor,
            OperationHandle handle,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        // create materialized table first
        operationExecutor.callExecutableOperation(handle, createMaterializedTableOperation);

        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();

        // Set job name, runtime mode, checkpoint interval
        // TODO: Set minibatch related optimization options.
        Configuration customConfig = new Configuration();
        String jobName =
                String.format(
                        "Materialized_table_%s_continuous_refresh_job",
                        materializedTableIdentifier.asSerializableString());
        customConfig.set(NAME, jobName);
        customConfig.set(RUNTIME_MODE, STREAMING);
        customConfig.set(CHECKPOINTING_INTERVAL, catalogMaterializedTable.getFreshness());

        String insertStatement =
                String.format(
                        "INSERT INTO %s %s",
                        materializedTableIdentifier, catalogMaterializedTable.getDefinitionQuery());
        try {
            // submit flink streaming job
            ResultFetcher resultFetcher =
                    operationExecutor.executeStatement(handle, insertStatement);

            // get execution.target and jobId, currently doesn't support yarn and k8s, so doesn't
            // get clusterId
            List<RowData> results = fetchAllResults(resultFetcher);
            String jobId = results.get(0).getString(0).toString();
            String executeTarget =
                    operationExecutor.getSessionContext().getSessionConf().get(TARGET);
            ContinuousRefreshHandler continuousRefreshHandler =
                    new ContinuousRefreshHandler(executeTarget, jobId);
            byte[] serializedBytes =
                    ContinuousRefreshHandlerSerializer.INSTANCE.serialize(continuousRefreshHandler);

            // update RefreshHandler to Catalog
            CatalogMaterializedTable updatedMaterializedTable =
                    catalogMaterializedTable.copy(
                            CatalogMaterializedTable.RefreshStatus.ACTIVATED,
                            continuousRefreshHandler.asSummaryString(),
                            serializedBytes);
            List<TableChange> tableChanges = new ArrayList<>();
            tableChanges.add(
                    TableChange.modifyRefreshStatus(
                            CatalogMaterializedTable.RefreshStatus.ACTIVATED));
            tableChanges.add(
                    TableChange.modifyRefreshHandler(
                            continuousRefreshHandler.asSummaryString(), serializedBytes));

            AlterMaterializedTableChangeOperation alterMaterializedTableChangeOperation =
                    new AlterMaterializedTableChangeOperation(
                            materializedTableIdentifier, tableChanges, updatedMaterializedTable);
            operationExecutor.callExecutableOperation(
                    handle, alterMaterializedTableChangeOperation);
        } catch (Exception e) {
            // drop materialized table while submit flink streaming job occur exception. Thus, weak
            // atomicity is guaranteed
            operationExecutor.callExecutableOperation(
                    handle,
                    new DropMaterializedTableOperation(materializedTableIdentifier, true, false));
            // log and throw exception
            LOG.error(
                    "Submit continuous refresh job for materialized table {} occur exception.",
                    materializedTableIdentifier,
                    e);
            throw new TableException(
                    String.format(
                            "Submit continuous refresh job for materialized table %s occur exception.",
                            materializedTableIdentifier),
                    e);
        }
    }

    private static List<RowData> fetchAllResults(ResultFetcher resultFetcher) {
        Long token = 0L;
        List<RowData> results = new ArrayList<>();
        while (token != null) {
            ResultSet result = resultFetcher.fetchResults(token, Integer.MAX_VALUE);
            results.addAll(result.getData());
            token = result.getNextToken();
        }
        return results;
    }

    private void createMaterializedInFullMode(
            WorkflowScheduler workflowScheduler,
            CreateMaterializedTableOperation createMaterializedTableOperation) {
        ObjectIdentifier materializedTableIdentifier =
                createMaterializedTableOperation.getTableIdentifier();
        CatalogMaterializedTable catalogMaterializedTable =
                createMaterializedTableOperation.getCatalogMaterializedTable();
        CreatePeriodicRefreshWorkflow event =
                new CreatePeriodicRefreshWorkflow(
                        materializedTableIdentifier,
                        "",
                        "",
                        "",
                        catalogMaterializedTable.getFreshness(),
                        Collections.emptyMap());
        try {
            RefreshHandler refreshHandler = workflowScheduler.createRefreshWorkflow(event);
            RefreshHandlerSerializer refreshHandlerSerializer =
                    workflowScheduler.getRefreshHandlerSerializer();
            byte[] refreshHandlerBytes = refreshHandlerSerializer.serialize(refreshHandler);
        } catch (Exception e) {
            throw new SqlExecutionException("Create refresh workflow occur exception", e);
        }
    }

    private void suspendMaterializedTableInFullMode(
            WorkflowScheduler workflowScheduler,
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable)
            throws Exception {
        RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        RefreshHandler refreshHandler =
                refreshHandlerSerializer.deserialize(
                        catalogMaterializedTable.getSerializedRefreshHandler(),
                        Thread.currentThread().getContextClassLoader());
        SuspendRefreshWorkflow<?> suspendRefreshWorkflow =
                new SuspendRefreshWorkflow<>(refreshHandler);
        workflowScheduler.modifyRefreshWorkflow(suspendRefreshWorkflow);
    }

    private void resumeMaterializeTableInFullMode(
            WorkflowScheduler workflowScheduler,
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable)
            throws Exception {
        RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        RefreshHandler refreshHandler =
                refreshHandlerSerializer.deserialize(
                        catalogMaterializedTable.getSerializedRefreshHandler(),
                        Thread.currentThread().getContextClassLoader());
        ResumeRefreshWorkflow<?> resumeRefreshWorkflow =
                new ResumeRefreshWorkflow<>(refreshHandler);
        workflowScheduler.modifyRefreshWorkflow(resumeRefreshWorkflow);
    }

    private void modifyMaterializedTableCronExprInFullMode(
            WorkflowScheduler workflowScheduler,
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable)
            throws Exception {
        RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        RefreshHandler refreshHandler =
                refreshHandlerSerializer.deserialize(
                        catalogMaterializedTable.getSerializedRefreshHandler(),
                        Thread.currentThread().getContextClassLoader());
        ModifyRefreshWorkflowCronExpr<?> modifyRefreshWorkflowCronExpr =
                new ModifyRefreshWorkflowCronExpr<>("", refreshHandler);
        workflowScheduler.modifyRefreshWorkflow(modifyRefreshWorkflowCronExpr);
    }

    private void deleteMaterializedTableWorkflowInFullMode(
            WorkflowScheduler workflowScheduler,
            ObjectIdentifier materializedTableIdentifier,
            CatalogMaterializedTable catalogMaterializedTable)
            throws Exception {
        RefreshHandlerSerializer refreshHandlerSerializer =
                workflowScheduler.getRefreshHandlerSerializer();
        RefreshHandler refreshHandler =
                refreshHandlerSerializer.deserialize(
                        catalogMaterializedTable.getSerializedRefreshHandler(),
                        Thread.currentThread().getContextClassLoader());
        DeleteRefreshWorkflow<?> deleteRefreshWorkflow = new DeleteRefreshWorkflow(refreshHandler);
        workflowScheduler.deleteRefreshWorkflow(deleteRefreshWorkflow);
    }
}
