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
import org.apache.gluten.integration.tpc.action.Dim;
import org.apache.gluten.integration.tpc.action.DimValue;
import org.apache.commons.lang3.ArrayUtils;
import picocli.CommandLine;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@CommandLine.Command(name = "parameterized", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Run queries with parameterized configurations")
public class Parameterized implements Callable<Integer> {
  @CommandLine.Mixin
  private TpcMixin mixin;

  @CommandLine.Mixin
  private DataGenMixin dataGenMixin;

  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-separated list of query IDs to run, run all queries if not specified. Example: --queries=q1,q6", split = ",")
  private String[] queries = new String[0];

  @CommandLine.Option(names = {"--excluded-queries"}, description = "Set a comma-separated list of query IDs to exclude. Example: --exclude-queries=q1,q6", split = ",")
  private String[] excludedQueries = new String[0];

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  @CommandLine.Option(names = {"--warmup-iterations"}, description = "Dry-run iterations before actually run the test", defaultValue = "0")
  private int warmupIterations;

  @CommandLine.Option(names = {"-m", "--metric"}, description = "Specify a series of metrics to collect during execution")
  private String[] metrics = new String[0];

  @CommandLine.Option(names = {"-d", "--dim"}, description = "Set a series of dimensions consisting of possible config options, example: -d=offheap:1g,spark.memory.offHeap.enabled=true,spark.memory.offHeap.size=1g")
  private String[] dims = new String[0];

  private static final Pattern dimPattern1 = Pattern.compile("([\\w-]+):([^,:]+)((?:,[^=,]+=[^=,]+)+)");
  private static final Pattern dimPattern2 = Pattern.compile("([^,:]+)((?:,[^=,]+=[^=,]+)+)");

  @Override
  public Integer call() throws Exception {
    final Map<String, Map<String, List<Map.Entry<String, String>>>> parsed = new HashMap<>();

    // parse dims
    for (String dim : dims) {
      Matcher matcher1 = dimPattern1.matcher(dim);
      Matcher matcher2 = dimPattern2.matcher(dim);
      if (matcher1.matches() == matcher2.matches()) {
        throw new IllegalArgumentException("Unexpected dim: " + dim);
      }
      final String dimName;
      final String dimValueName;
      final String confText;

      if (matcher1.matches()) {
        dimName = matcher1.group(1);
        dimValueName = matcher1.group(2);
        confText = matcher1.group(3).substring(1); // trim leading ","
      } else {
        // matcher2.matches
        dimName = matcher2.group(1);
        dimValueName = matcher2.group(0);
        confText = matcher2.group(2).substring(1); // trim leading ","
      }

      final List<Map.Entry<String, String>> options = new ArrayList<>();
      String[] splits = confText.split(",");
      if (splits.length == 0) {
        throw new IllegalArgumentException("Unexpected dim: " + dim);
      }
      for (String split : splits) {
        String[] kv = split.split("=");
        if (kv.length != 2) {
          throw new IllegalArgumentException("Unexpected dim: " + dim);
        }
        options.add(new AbstractMap.SimpleImmutableEntry<>(kv[0], kv[1]));
      }

      parsed.computeIfAbsent(dimName, s -> new LinkedHashMap<>())
          .computeIfAbsent(dimValueName, s -> new ArrayList<>())
          .addAll(options);
    }

    // Convert Map<String, Map<String, List<Map.Entry<String, String>>>> to List<Dim>
    Seq<Dim> parsedDims = JavaConverters.asScalaBufferConverter(
        parsed.entrySet().stream().map(e ->
            new Dim(e.getKey(), JavaConverters.asScalaBufferConverter(
                e.getValue().entrySet().stream().map(e2 ->
                    new DimValue(e2.getKey(), JavaConverters.asScalaBufferConverter(
                        e2.getValue().stream().map(e3 -> new Tuple2<>(e3.getKey(), e3.getValue()))
                            .collect(Collectors.toList())).asScala())).collect(Collectors.toList())).asScala()
            )).collect(Collectors.toList())).asScala();

    org.apache.gluten.integration.tpc.action.Parameterized parameterized =
        new org.apache.gluten.integration.tpc.action.Parameterized(dataGenMixin.getScale(), this.queries, excludedQueries, iterations, warmupIterations, parsedDims, metrics);
    return mixin.runActions(ArrayUtils.addAll(dataGenMixin.makeActions(), parameterized));
  }
}
