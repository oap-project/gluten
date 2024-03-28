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

import org.apache.gluten.integration.tpc.action.Action;
import picocli.CommandLine;

public class DataGenMixin {
  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--gen-partitioned-data"}, description = "Generate data with partitions", defaultValue = "false")
  private boolean genPartitionedData;

  @CommandLine.Option(names = {"--skip-data-gen"}, description = "Skip data generation", defaultValue = "false")
  private boolean skipDataGen;

  public Action[] makeActions() {
    if (skipDataGen) {
      return new Action[0];
    }
    return new Action[]{new org.apache.gluten.integration.tpc.action.DataGenOnly(scale, genPartitionedData)};
  }

  public double getScale() {
    return scale;
  }
}
