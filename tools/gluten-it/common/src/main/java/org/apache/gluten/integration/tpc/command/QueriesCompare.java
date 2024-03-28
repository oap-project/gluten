/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.integration.tpc.command;

import org.apache.gluten.integration.tpc.TpcMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "queries-compare", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Run queries and do result comparison with baseline preset.")
public class QueriesCompare implements Callable<Integer> {
  @CommandLine.Mixin
  private TpcMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(names = {"--excluded-queries"}, description = "Set a comma-separated list of query IDs to exclude. Example: --exclude-queries=q1,q6", split = ",")
  private String[] excludedQueries = new String[0];

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  @Override
  public Integer call() throws Exception {
    org.apache.gluten.integration.tpc.action.QueriesCompare queriesCompare =
        new org.apache.gluten.integration.tpc.action.QueriesCompare(dataGenMixin.getScale(), this.queries, this.excludedQueries, explain, iterations);
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), queriesCompare));
  }
}
