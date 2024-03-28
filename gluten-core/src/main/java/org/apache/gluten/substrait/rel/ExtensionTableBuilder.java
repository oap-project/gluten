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
package org.apache.gluten.substrait.rel;

import java.util.List;
import java.util.Map;

public class ExtensionTableBuilder {
  private ExtensionTableBuilder() {}

  public static ExtensionTableNode makeExtensionTable(
      Long minPartsNum,
      Long maxPartsNum,
      String database,
      String tableName,
      String relativeTablePath,
      String absoluteTablePath,
      String orderByKey,
      String lowCardKey,
      String minmaxIndexKey,
      String bfIndexKey,
      String setIndexKey,
      String primaryKey,
      List<String> partList,
      List<Long> starts,
      List<Long> lengths,
      String tableSchemaJson,
      Map<String, String> clickhouseTableConfigs,
      List<String> preferredLocations) {
    return new ExtensionTableNode(
        minPartsNum,
        maxPartsNum,
        database,
        tableName,
        relativeTablePath,
        absoluteTablePath,
        orderByKey,
        lowCardKey,
        minmaxIndexKey,
        bfIndexKey,
        setIndexKey,
        primaryKey,
        partList,
        starts,
        lengths,
        tableSchemaJson,
        clickhouseTableConfigs,
        preferredLocations);
  }
}
