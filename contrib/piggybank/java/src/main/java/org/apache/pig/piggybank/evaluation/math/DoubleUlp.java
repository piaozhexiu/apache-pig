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

package org.apache.pig.piggybank.evaluation.math;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.builtin.Unique;
import org.apache.pig.data.Tuple;

/**
 * math.ULP implements a binding to the Java function
* {@link java.lang.Math#ulp(double) Math.ulp(double)}. 
* Given a single data atom it Returns the size of an ulp 
* of the argument.
* 
* <dl>
* <dt><b>Parameters:</b></dt>
* <dd><code>value</code> - <code>Double</code>.</dd>
* 
* <dt><b>Return Value:</b></dt>
* <dd><code>Double</code> </dd>
* 
* <dt><b>Return Schema:</b></dt>
* <dd>ULP_inputSchema</dd>
* 
* <dt><b>Example:</b></dt>
* <dd><code>
* register math.jar;<br/>
* A = load 'mydata' using PigStorage() as ( float1 );<br/>
* B = foreach A generate float1, math.ULP(float1);
* </code></dd>
* </dl>
* 
* @see Math#ulp(double)
* @see
* @author ajay garg
*
*/
@OutputSchema("double")
@Unique
public class DoubleUlp extends EvalFunc<Double>{
	/**
	 * java level API
	 * @param input expects a single numeric value
	 * @param output returns a single numeric value, 
	 * the size of an ulp of the argument.
	 */
	public Double exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;

        try{
            Double d = (Double)input.get(0);
		    return Math.ulp(d);
        } catch(Exception e){
            throw new IOException("Caught exception in DoubleUlp", e);
        }
    }
}
