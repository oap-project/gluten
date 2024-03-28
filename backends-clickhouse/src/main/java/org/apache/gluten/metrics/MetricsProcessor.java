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
package org.apache.gluten.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetricsProcessor {

  protected String name;
  protected long time;

  @JsonProperty("input_rows")
  protected long inputRows = 0;

  protected long inputVectors = 0;

  @JsonProperty("input_bytes")
  protected long inputBytes = 0;

  @JsonProperty("output_rows")
  protected long outputRows = 0;

  protected long outputVectors = 0;

  @JsonProperty("output_bytes")
  protected long outputBytes = 0;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getInputRows() {
    return inputRows;
  }

  public void setInputRows(long inputRows) {
    this.inputRows = inputRows;
  }

  public long getInputVectors() {
    return inputVectors;
  }

  public void setInputVectors(long inputVectors) {
    this.inputVectors = inputVectors;
  }

  public long getInputBytes() {
    return inputBytes;
  }

  public void setInputBytes(long inputBytes) {
    this.inputBytes = inputBytes;
  }

  public long getOutputRows() {
    return outputRows;
  }

  public void setOutputRows(long outputRows) {
    this.outputRows = outputRows;
  }

  public long getOutputVectors() {
    return outputVectors;
  }

  public void setOutputVectors(long outputVectors) {
    this.outputVectors = outputVectors;
  }

  public long getOutputBytes() {
    return outputBytes;
  }

  public void setOutputBytes(long outputBytes) {
    this.outputBytes = outputBytes;
  }
}
