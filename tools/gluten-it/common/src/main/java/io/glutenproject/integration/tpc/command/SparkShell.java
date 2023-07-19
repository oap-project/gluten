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
package io.glutenproject.integration.tpc.command;

import io.glutenproject.integration.tpc.TpcMixin;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "spark-shell", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Open a standard Spark shell.")
public class SparkShell implements Callable<Integer> {
  @CommandLine.Mixin
  private TpcMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @Override
  public Integer call() throws Exception {
    io.glutenproject.integration.tpc.action.SparkShell sparkShell =
        new io.glutenproject.integration.tpc.action.SparkShell(dataGenMixin.getScale());
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), sparkShell));
  }
}
