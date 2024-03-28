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
package org.apache.gluten.memory.memtarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// For debugging purpose only
public class LoggingMemoryTarget implements MemoryTarget {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingMemoryTarget.class);
  private final MemoryTarget delegated;

  public LoggingMemoryTarget(MemoryTarget delegated) {
    this.delegated = delegated;
  }

  @Override
  public long borrow(long size) {
    long before = usedBytes();
    long reserved = delegated.borrow(size);
    long after = usedBytes();
    LOGGER.info(
        String.format(
            "Borrowed[%s]: %d + %d(%d) = %d", this.toString(), before, reserved, size, after));
    return reserved;
  }

  @Override
  public long repay(long size) {
    long before = usedBytes();
    long unreserved = delegated.repay(size);
    long after = usedBytes();
    LOGGER.info(
        String.format(
            "Repaid[%s]: %d - %d(%d) = %d", this.toString(), before, unreserved, size, after));
    return unreserved;
  }

  @Override
  public long usedBytes() {
    return delegated.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget delegated() {
    return delegated;
  }
}
