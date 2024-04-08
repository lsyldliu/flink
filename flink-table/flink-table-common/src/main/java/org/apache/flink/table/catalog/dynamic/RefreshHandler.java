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

package org.apache.flink.table.catalog.dynamic;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface represents the meta information of current dynamic table background refresh
 * pipeline. The refresh mode maybe continuous or full. The format of the meta information in the
 * two modes is not consistent, so user need to implementation this interface according to .
 *
 * <p>This meta information will be serialized to bytes by {@link RefreshHandlerSerializer}, then
 * store to Catalog for suspend or drop {@link CatalogDynamicTable}.
 *
 * <p>In continuous mode, the format of the meta information maybe { "clusterType": "yarn",
 * "clusterId": "xxx", "jobId": "yyyy" }.
 *
 * <p>In full mode, the meta information format maybe { "endpoint": "xxx", "workflowId": "yyy" }.
 * Due to you may use different workflow scheduler plugin in this mode, you should implement this
 * interface according to your plugin.
 */
@PublicEvolving
public interface RefreshHandler {

    /** Returns a string that summarizes this refresh handler meta information. */
    String asSummaryString();
}
