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

package org.apache.flink.table.gateway.service.inmemory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.dynamic.RefreshHandlerSerializer;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.table.workflow.WorkflowScheduler;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A mocked in-memory workflow scheduler for demo. */
@Internal
public class InMemoryWorkflowScheduler implements WorkflowScheduler<InMemoryRefreshHandler> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryWorkflowScheduler.class);

    public static final String SCHEDULE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern(SCHEDULE_TIME_FORMAT);

    private final Map<ObjectIdentifier, InMemoryRefreshHandler> dynamicTableRefreshHandlers =
            new HashMap<>();
    private final Map<ObjectIdentifier, ScheduledFuture<?>> dynamicTableScheduledFutures =
            new HashMap<>();

    private SqlGatewayService service;
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public void start(SqlGatewayService service) {
        this.service = checkNotNull(service);
        this.scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(
                        10, new ExecutorThreadFactory("InMemory Workflow Scheduler Pool"));
    }

    public void stop() {
        if (scheduledThreadPoolExecutor != null) {
            scheduledThreadPoolExecutor.shutdown();
            LOG.info("Shutdown InMemory workflow scheduler successfully.");
        }
        dynamicTableRefreshHandlers.clear();
        dynamicTableScheduledFutures.clear();
    }

    @Override
    public RefreshHandlerSerializer createSerializer() {
        return InMemoryRefreshHandlerSerializer.INSTANCE;
    }

    @Override
    public InMemoryRefreshHandler createRefreshWorkflow(
            ObjectIdentifier dynamicTableIdentifier,
            String refreshStatement,
            String workflowNamePrefix,
            String refreshCron,
            boolean isPeriodic,
            String triggerUrl,
            Map<String, String> staticPartitions,
            Map<String, String> executionConf) {
        if (dynamicTableRefreshHandlers.containsKey(dynamicTableIdentifier)) {
            LOG.info(
                    "Dynamic Table {} refresh handler {} has been created before.",
                    dynamicTableIdentifier.asSummaryString(),
                    dynamicTableRefreshHandlers.get(dynamicTableIdentifier));
            return dynamicTableRefreshHandlers.get(dynamicTableIdentifier);
        }
        // assume the refreshCron always is a long value for test
        long interval = Long.parseLong(refreshCron);
        ScheduledFuture<?> scheduledFuture =
                scheduledThreadPoolExecutor.scheduleAtFixedRate(
                        new ScheduledTask(
                                service,
                                dynamicTableIdentifier,
                                isPeriodic,
                                staticPartitions,
                                executionConf),
                        interval,
                        interval,
                        TimeUnit.SECONDS);

        String createTime = formatTimeStamp(System.currentTimeMillis());
        InMemoryRefreshHandler inMemoryRefreshHandler =
                new InMemoryRefreshHandler(
                        dynamicTableIdentifier,
                        refreshStatement,
                        workflowNamePrefix + createTime,
                        interval,
                        isPeriodic,
                        staticPartitions,
                        executionConf);
        // put to map
        dynamicTableRefreshHandlers.put(dynamicTableIdentifier, inMemoryRefreshHandler);
        dynamicTableScheduledFutures.put(dynamicTableIdentifier, scheduledFuture);
        return inMemoryRefreshHandler;
    }

    public boolean suspendRefreshWorkflow(InMemoryRefreshHandler refreshHandler) {
        ObjectIdentifier dynamicTableIdentifier = refreshHandler.getDynamicTableIdentifier();
        if (!dynamicTableRefreshHandlers.containsKey(dynamicTableIdentifier)) {
            LOG.info(
                    "Dynamic table {} refresh workflow {} has been deleted.",
                    dynamicTableIdentifier.asSummaryString(),
                    refreshHandler.asSummaryString());
            return false;
        }
        if (!dynamicTableScheduledFutures.containsKey(dynamicTableIdentifier)) {
            LOG.info(
                    "Dynamic table {} refresh task {} has been suspended or deleted.",
                    dynamicTableIdentifier.asSummaryString(),
                    refreshHandler.asSummaryString());
            return false;
        }
        ScheduledFuture<?> scheduledFuture =
                dynamicTableScheduledFutures.get(dynamicTableIdentifier);
        scheduledFuture.cancel(true);
        LOG.info(
                "Suspend dynamic table {} background refresh task successfully.",
                dynamicTableIdentifier.asSummaryString());

        dynamicTableScheduledFutures.remove(dynamicTableIdentifier);
        return true;
    }

    @Override
    public boolean resumeRefreshWorkflow(
            InMemoryRefreshHandler refreshHandler, Map<String, String> updatedExecutionConf) {
        ObjectIdentifier dynamicTableIdentifier = refreshHandler.getDynamicTableIdentifier();
        if (!dynamicTableRefreshHandlers.containsKey(dynamicTableIdentifier)) {
            LOG.info(
                    "Dynamic table {} refresh workflow {} has been deleted.",
                    dynamicTableIdentifier.asSummaryString(),
                    refreshHandler.asSummaryString());
            return false;
        }
        if (dynamicTableScheduledFutures.containsKey(dynamicTableIdentifier)) {
            LOG.info(
                    "Dynamic table {} refresh task {} is running now, doesn't need to resume again.",
                    dynamicTableIdentifier.asSummaryString(),
                    refreshHandler.asSummaryString());
            return false;
        }

        long interval = refreshHandler.getSchedulerInterval();
        ScheduledFuture<?> scheduledFuture =
                scheduledThreadPoolExecutor.scheduleAtFixedRate(
                        new ScheduledTask(
                                service,
                                dynamicTableIdentifier,
                                refreshHandler.isPeriodic(),
                                refreshHandler.getStaticPartitions(),
                                updatedExecutionConf),
                        interval,
                        interval,
                        TimeUnit.SECONDS);
        // put ScheduledFuture into map
        dynamicTableScheduledFutures.put(dynamicTableIdentifier, scheduledFuture);
        return true;
    }

    @Override
    public boolean deleteRefreshWorkflow(InMemoryRefreshHandler refreshHandler) {
        ObjectIdentifier dynamicTableIdentifier = refreshHandler.getDynamicTableIdentifier();
        if (!dynamicTableRefreshHandlers.containsKey(dynamicTableIdentifier)) {
            LOG.warn(
                    "Failed to delete dynamic table {} refresh workflow {}. It maybe has been deleted before.",
                    dynamicTableIdentifier.asSerializableString(),
                    refreshHandler.asSummaryString());
            return false;
        }

        if (dynamicTableScheduledFutures.containsKey(dynamicTableIdentifier)) {
            ScheduledFuture<?> scheduledFuture =
                    dynamicTableScheduledFutures.get(dynamicTableIdentifier);
            scheduledFuture.cancel(true);
            LOG.info("Stop dynamic table background refresh task successfully.");
        } else {
            LOG.warn(
                    "Dynamic table {} background refresh task has been stopped, so just delete refresh handler.",
                    dynamicTableIdentifier.asSerializableString(),
                    refreshHandler.asSummaryString());
        }

        // Remove the refresh handler
        dynamicTableRefreshHandlers.remove(dynamicTableIdentifier);
        dynamicTableScheduledFutures.remove(dynamicTableIdentifier);
        return true;
    }

    private static final class ScheduledTask implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(ScheduledTask.class);

        private final SqlGatewayService service;
        private final ObjectIdentifier dynamicTableIdentifier;
        private final boolean isPeriodic;
        private final Map<String, String> staticPartitions;
        private final Map<String, String> executionConfig;

        public ScheduledTask(
                SqlGatewayService service,
                ObjectIdentifier dynamicTableIdentifier,
                boolean isPeriodic,
                Map<String, String> staticPartitions,
                Map<String, String> executionConfig) {
            this.service = service;
            this.dynamicTableIdentifier = dynamicTableIdentifier;
            this.isPeriodic = isPeriodic;
            this.staticPartitions = staticPartitions;
            this.executionConfig = executionConfig;
        }

        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            String scheduleTime = formatTimeStamp(currentTime);
            SessionHandle sessionHandle =
                    service.openSession(
                            SessionEnvironment.newBuilder()
                                    .setSessionEndpointVersion(SqlGatewayRestAPIVersion.V3)
                                    .build());
            service.refreshDynamicTable(
                    sessionHandle,
                    dynamicTableIdentifier,
                    isPeriodic,
                    scheduleTime,
                    SCHEDULE_TIME_FORMAT,
                    staticPartitions,
                    executionConfig);
            LOG.info(
                    "Trigger dynamic table {} refresh operation at time: {}",
                    dynamicTableIdentifier.asSummaryString(),
                    scheduleTime);
        }
    }

    public static String formatTimeStamp(long timeMillis) {
        return formatTimeStamp(timeMillis, DEFAULT_DATETIME_FORMATTER);
    }

    public static String formatTimeStamp(long timeMillis, DateTimeFormatter dateTimeFormatter) {
        Objects.requireNonNull(dateTimeFormatter);
        return dateTimeFormatter.format(
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis), ZoneId.systemDefault()));
    }
}
