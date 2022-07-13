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

package io.glutenproject.memory;

import java.util.concurrent.atomic.AtomicLong;

public final class NativeSQLMemoryMetrics {
  private final AtomicLong peak = new AtomicLong(0L);
  private final AtomicLong total = new AtomicLong(0L);

  public void inc(long bytes) {
    final long total = this.total.addAndGet(bytes);
    long prev_peak;
    do {
      prev_peak = this.peak.get();
      if (total <= prev_peak) {
        break;
      }
    } while (!this.peak.compareAndSet(prev_peak, total));
  }

  public long peak() {
    return peak.get();
  }

  public long total() {
    return total.get();
  }
}
