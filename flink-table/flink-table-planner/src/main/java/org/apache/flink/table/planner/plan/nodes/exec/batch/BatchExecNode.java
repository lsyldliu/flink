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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.codegen.fusion.OperatorFusionCodegenSupport;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;

/** Base class for batch {@link ExecNode}. */
@Internal
public interface BatchExecNode<T> extends ExecNode<T> {

    default boolean supportMultipleCodegen() {
        return false;
    }

    default OperatorFusionCodegenSupport getInputCodegenOp(
            int multipleInputId, PlannerBase planner, ExecNodeConfig config) {
        throw new TableException(
                "This node doesn't support as input node of multiple operator fusion codegen.");
    }
}
