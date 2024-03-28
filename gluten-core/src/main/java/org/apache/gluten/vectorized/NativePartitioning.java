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
package org.apache.gluten.vectorized;

import java.io.Serializable;

/** POJO to hold partitioning parameters needed by native shuffle writer */
public class NativePartitioning implements Serializable {

  private final String shortName;
  private final int numPartitions;

  private final byte[] exprList;

  private final byte[] requiredFields;

  private final byte[] schema;

  /**
   * Constructs a new instance.
   *
   * @param shortName Partitioning short name. "single" -> SinglePartitioning, "rr" ->
   *     RoundRobinPartitioning, "hash" -> HashPartitioning, "range" -> RangePartitioning
   * @param numPartitions Partitioning numPartitions
   * @param exprList Serialized expressions
   */
  public NativePartitioning(String shortName, int numPartitions, byte[] exprList) {
    this.shortName = shortName;
    this.numPartitions = numPartitions;
    this.exprList = exprList;
    this.schema = null;
    this.requiredFields = null;
  }

  public NativePartitioning(String shortName, int numPartitions) {
    this(shortName, numPartitions, null, null, null);
  }

  public NativePartitioning(String shortName, int numPartitions, byte[] schema, byte[] exprList) {
    this.shortName = shortName;
    this.numPartitions = numPartitions;
    this.schema = schema;
    this.exprList = exprList;
    this.requiredFields = null;
  }

  public NativePartitioning(
      String shortName, int numPartitions, byte[] schema, byte[] exprList, byte[] requiredFields) {
    this.shortName = shortName;
    this.numPartitions = numPartitions;
    this.schema = schema;
    this.exprList = exprList;
    this.requiredFields = requiredFields;
  }

  public String getShortName() {
    return shortName;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public byte[] getExprList() {
    return exprList;
  }

  public byte[] getRequiredFields() {
    return requiredFields;
  }

  public byte[] getSchema() {
    return schema;
  }
}
