/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.builtin;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import org.apache.pig.classification.InterfaceAudience;
import org.apache.pig.classification.InterfaceStability;

/**
 * An EvalFunc annotation which can be used together with {@link OutputSchema @OutputSchema}.
 * It takes an array of comma separated schema fields and turns them into unique field names
 * <br>
 * Example:<br>
 * <pre>
 * {@literal @}OutputSchema("t:(i:int)")
 * {@literal @}Unique({"t", "i"})
 * public class MyFunc extends EvalFunc {...}
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Unique {
    public String[] value() default {};
}
