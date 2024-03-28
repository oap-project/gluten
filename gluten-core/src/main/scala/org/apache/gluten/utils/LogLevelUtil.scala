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
package org.apache.gluten.utils

import org.apache.spark.internal.Logging

trait LogLevelUtil { self: Logging =>

  protected def logOnLevel(level: String, msg: => String): Unit =
    level match {
      case "TRACE" => logTrace(msg)
      case "DEBUG" => logDebug(msg)
      case "INFO" => logInfo(msg)
      case "WARN" => logWarning(msg)
      case "ERROR" => logError(msg)
      case _ => logDebug(msg)
    }

  protected def logOnLevel(level: String, msg: => String, e: Throwable): Unit =
    level match {
      case "TRACE" => logTrace(msg, e)
      case "DEBUG" => logDebug(msg, e)
      case "INFO" => logInfo(msg, e)
      case "WARN" => logWarning(msg, e)
      case "ERROR" => logError(msg, e)
      case _ => logDebug(msg, e)
    }
}
