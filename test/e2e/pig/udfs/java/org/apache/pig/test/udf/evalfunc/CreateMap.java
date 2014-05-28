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

package org.apache.pig.test.udf.evalfunc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.builtin.Unique;
import org.apache.pig.data.Tuple;

@OutputSchema("createmap:map")
@Unique
public class CreateMap extends EvalFunc<Map<String, Object> >
{
	//@Override
	public Map<String, Object> exec(Tuple input) throws IOException 
	{
		if (input == null || input.size() < 2)
		{
			return null;
		}

		String key;
		Object val;
		try{
        		key = (String)input.get(0);
        		val = input.get(1);
		}catch(Exception e){
			System.err.println("Failed to process input data; error: " + e.getMessage());
			return null;
		}

		HashMap<String, Object> output = new HashMap<String, Object>(1);
		output.put(key, val);

		return output;
        }
}
