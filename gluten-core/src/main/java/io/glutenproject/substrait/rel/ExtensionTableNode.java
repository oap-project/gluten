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
package io.glutenproject.substrait.rel;

import io.glutenproject.backendsapi.BackendsApiManager;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.StringValue;
import io.substrait.proto.ReadRel;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExtensionTableNode implements SplitInfo {
  private static final String MERGE_TREE = "MergeTree;";
  private Long minPartsNum;
  private Long maxPartsNum;
  private String database;
  private String tableName;
  private String relativePath;
  private String absolutePath;
  private String tableSchemaJson;
  private StringBuffer extensionTableStr = new StringBuffer(MERGE_TREE);
  private StringBuffer partPathList = new StringBuffer("");
  private final List<String> preferredLocations = new ArrayList<>();

  private String orderByKey;

  private String primaryKey;

  private String lowCardKey;

  private List<String> partList;
  private List<Long> starts;
  private List<Long> lengths;

  private Map<String, String> clickhouseTableConfigs;

  ExtensionTableNode(
      Long minPartsNum,
      Long maxPartsNum,
      String database,
      String tableName,
      String relativePath,
      String absolutePath,
      String orderByKey,
      String lowCardKey,
      String primaryKey,
      List<String> partList,
      List<Long> starts,
      List<Long> lengths,
      String tableSchemaJson,
      Map<String, String> clickhouseTableConfigs,
      List<String> preferredLocations) {
    this.minPartsNum = minPartsNum;
    this.maxPartsNum = maxPartsNum;
    this.database = database;
    this.tableName = tableName;
    URI table_uri = URI.create(relativePath);
    if (table_uri.getPath().startsWith("/")) { // file:///tmp/xxx => tmp/xxx
      this.relativePath = table_uri.getPath().substring(1);
    } else {
      this.relativePath = table_uri.getPath();
    }
    this.absolutePath = absolutePath;
    this.tableSchemaJson = tableSchemaJson;
    this.orderByKey = orderByKey;
    this.lowCardKey = lowCardKey;
    this.primaryKey = primaryKey;
    this.partList = partList;
    this.starts = starts;
    this.lengths = lengths;
    this.clickhouseTableConfigs = clickhouseTableConfigs;
    this.preferredLocations.addAll(preferredLocations);

    // New: MergeTree;{database}\n{table}\n{orderByKey}\n{primaryKey}\n{relative_path}\n
    // {part_path1}\n{part_path2}\n...
    long end = 0;
    for (int i = 0; i < this.partList.size(); i++) {
      end = this.starts.get(i) + this.lengths.get(i);
      partPathList
          .append(this.partList.get(i))
          .append("\n")
          .append(this.starts.get(i))
          .append("\n")
          .append(end)
          .append("\n");
    }

    extensionTableStr
        .append(database)
        .append("\n")
        .append(tableName)
        .append("\n")
        .append(tableSchemaJson)
        .append("\n")
        .append(this.orderByKey)
        .append("\n");

    if (!this.orderByKey.isEmpty() && !this.orderByKey.equals("tuple()")) {
      extensionTableStr.append(this.primaryKey).append("\n");
    }
    extensionTableStr.append(this.lowCardKey).append("\n");
    extensionTableStr.append(this.relativePath).append("\n");
    extensionTableStr.append(this.absolutePath).append("\n");

    if (this.clickhouseTableConfigs != null && !this.clickhouseTableConfigs.isEmpty()) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        String clickhouseTableConfigsJson =
            objectMapper
                .writeValueAsString(this.clickhouseTableConfigs)
                .replaceAll("\\\n", "")
                .replaceAll(" ", "");
        extensionTableStr.append(clickhouseTableConfigsJson).append("\n");
      } catch (Exception e) {
        extensionTableStr.append("").append("\n");
      }
    } else {
      extensionTableStr.append("").append("\n");
    }
    extensionTableStr.append(partPathList);
    /* old format
    if (!this.partList.isEmpty()) {
    } else {
      // Old: MergeTree;{database}\n{table}\n{relative_path}\n{min_part}\n{max_part}\n
      extensionTableStr
          .append(database)
          .append("\n")
          .append(tableName)
          .append("\n")
          .append(relativePath)
          .append("\n")
          .append(this.minPartsNum)
          .append("\n")
          .append(this.maxPartsNum)
          .append("\n");
    } */
  }

  @Override
  public List<String> preferredLocations() {
    return this.preferredLocations;
  }

  public ReadRel.ExtensionTable toProtobuf() {
    ReadRel.ExtensionTable.Builder extensionTableBuilder = ReadRel.ExtensionTable.newBuilder();
    StringValue extensionTable =
        StringValue.newBuilder().setValue(extensionTableStr.toString()).build();
    extensionTableBuilder.setDetail(
        BackendsApiManager.getTransformerApiInstance().packPBMessage(extensionTable));
    return extensionTableBuilder.build();
  }

  public String getRelativePath() {
    return relativePath;
  }

  public String getAbsolutePath() {
    return absolutePath;
  }

  public List<String> getPartList() {
    return partList;
  }
}
