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

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class SparkRunModes {
  public interface Mode {
    String getSparkMasterUrl();
    Map<String, String> extraSparkConf();
  }

  public static class ModeEnumeration implements Mode {
    @CommandLine.ArgGroup(exclusive = false)
    LocalMode localMode;

    @CommandLine.ArgGroup(exclusive = false)
    LocalClusterMode localClusterMode;

    private Mode getActiveMode() {
      int enabledModeCount = 0;
      if (localMode != null) {
        enabledModeCount++;
      }
      if (localClusterMode != null) {
        enabledModeCount++;
      }

      if (enabledModeCount != 1) {
        throw new IllegalStateException("Only one single run mode can be specified");
      }

      if (localMode != null) {
        return localMode;
      }

      if (localClusterMode != null) {
        return localClusterMode;
      }

      throw new IllegalStateException("unreachable code");
    }

    @Override
    public String getSparkMasterUrl() {
      return getActiveMode().getSparkMasterUrl();
    }

    @Override
    public Map<String, String> extraSparkConf() {
      return getActiveMode().extraSparkConf();
    }
  }

  private static class LocalMode implements Mode {
    @CommandLine.Option(names = {"--local"}, description = "Run in Spark local mode", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--threads"}, description = "Local mode: Run Spark locally with as many worker threads", defaultValue = "4")
    private int localThreads;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local mode");
      }

      return String.format("local[%d]", localThreads);
    }

    @Override
    public Map<String, String> extraSparkConf() {
      return Collections.emptyMap();
    }
  }

  private static class LocalClusterMode implements Mode {
    // We should transfer the jars to be tested in the integration testing to executors
    public static final String[] EXTRA_JARS = new String[]{"gluten-package-1.2.0-SNAPSHOT.jar"};

    @CommandLine.Option(names = {"--local-cluster"}, description = "Run in Spark local cluster mode", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--workers"}, description = "Local cluster mode: Number of workers", defaultValue = "2")
    private int lcWorkers;

    @CommandLine.Option(names = {"--worker-cores"}, description = "Local cluster mode: Number of cores per worker", defaultValue = "2")
    private int lcWorkerCores;

    @CommandLine.Option(names = {"--worker-mem"}, description = "Local cluster mode: Memory per worker", defaultValue = "4g")
    private String lcWorkerMem;

    @CommandLine.Option(names = {"--executor-cores"}, description = "Local cluster mode: Number of cores per executor", defaultValue = "1")
    private int lcExecutorCores;

    @CommandLine.Option(names = {"--executor-mem"}, description = "Local cluster mode: Memory per executor", defaultValue = "2g")
    private String lcExecutorMem;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local cluster mode");
      }
      if (!System.getenv().containsKey("SPARK_HOME")) {
        throw new IllegalArgumentException("SPARK_HOME not set! Please use --local if there is no local Spark build");
      }
      return String.format("local-cluster[%d,%d,%d]", lcWorkers, lcWorkerCores, Utils.byteStringAsMb(lcWorkerMem));
    }

    @Override
    public Map<String, String> extraSparkConf() {
      final Set<String> extraJarSet = Arrays.stream(EXTRA_JARS).collect(Collectors.toSet());
      String classpath = System.getProperty("java.class.path");
      String[] classPathValues = classpath.split(File.pathSeparator);
      Optional<String> extraClassPath = Arrays.stream(classPathValues).filter(classPath -> {
        File file = new File(classPath);
        return file.exists() && file.isFile() && extraJarSet.contains(file.getName());
      }).reduce((s1, s2) -> s1 + File.pathSeparator + s2);

      final Map<String, String> extras = new HashMap<>();
      extras.put(SparkLauncher.EXECUTOR_CORES, String.valueOf(lcExecutorCores));
      extras.put(SparkLauncher.EXECUTOR_MEMORY, lcExecutorMem);
      extraClassPath.ifPresent(path -> extras.put("spark.executor.extraClassPath", path));
      return extras;
    }
  }
}
