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
package org.apache.gluten.exception;

/**
 * Generally, this exception should be thrown if Gluten doesn't support an operation, but vanilla
 * Spark supports it. We expect Gluten makes such operation or its relevant operations fall back to
 * vanilla Spark when handling this exception.
 */
public class GlutenNotSupportException extends GlutenException {

  public GlutenNotSupportException() {}

  public GlutenNotSupportException(String message) {
    super(message);
  }

  public GlutenNotSupportException(Throwable t) {
    super(t);
  }

  public GlutenNotSupportException(String message, Throwable t) {
    super(message, t);
  }
}
