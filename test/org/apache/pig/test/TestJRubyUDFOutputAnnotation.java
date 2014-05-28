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
package org.apache.pig.test;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;

import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.ScriptingUDFOutputAnnotationBase;
import org.junit.Test;

public class TestJRubyUDFOutputAnnotation extends ScriptingUDFOutputAnnotationBase {

    private final ScriptingType type = ScriptingType.JRUBY;
   
    @Test
    public void testScript1() throws Exception {
      
      String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    def simple1 str\n" +
            "        return str\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.simple1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema expected = new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript2() throws Exception {
      
      String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    outputSchema \"my\"\n" +
            "    def simple1 str\n" +
            "        return str\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.simple1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.BYTEARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }

    @Test
    public void testScript3() throws Exception {
      
        String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    outputSchema \"my:chararray\"\n" +
            "    def simple2 str\n" +
            "        return str\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.simple2(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.CHARARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript4() throws Exception {
      
        String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    outputSchema \"t:tuple\", {:use_inputschema => true}\n" +
            "    def test_tup1 str\n" +
            "        return [str]\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.test_tup1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema input = new Schema(new Schema.FieldSchema("i", DataType.INTEGER));
        Schema expected = new Schema(new Schema.FieldSchema("t", input, DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);

    }

    @Test
    public void testScript5() throws Exception {
      
        String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    outputSchema \"t:tuple(num:long,${0}:chararray)\", {:use_inputschema => true, :unique_fields => nil}\n" +
            "    def test_tup2 str\n" +
            "        return [str.length, str]\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.test_tup2(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        int magic = 9;
        int currentSchemaId = getEvalFuncNextSchemaId() + magic;
        int inputSchemaId = ++currentSchemaId;
        Schema input = new Schema();
        input.add(new Schema.FieldSchema("num_" + ++inputSchemaId, DataType.LONG));
        input.add(new Schema.FieldSchema("test_tup2_i_" + ++inputSchemaId, DataType.CHARARRAY));
        
        Schema expected = new Schema(new Schema.FieldSchema("t_" + currentSchemaId, input, DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);
    }
    
    @Test
    public void testScript6() throws Exception {
      
        String scriptDef =
            "class Myudfs < PigUdf\n" +
            "    outputSchema \"t:tuple(num:long,${0}:chararray)\", {:use_inputschema => true, :unique_fields => [\"${0}\"]}\n" +
            "    def test_tup2 str\n" +
            "        return [str.length, str]\n" +
            "    end\n" +
            "end\n";
        
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jrubyudfs.test_tup2(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        int magic = 2;
        int currentSchemaId = getEvalFuncNextSchemaId() + magic;
        int inputSchemaId = ++currentSchemaId;
        Schema input = new Schema();
        input.add(new Schema.FieldSchema("num", DataType.LONG));
        input.add(new Schema.FieldSchema("test_tup2_i_" + ++inputSchemaId, DataType.CHARARRAY));
        
        Schema expected = new Schema(new Schema.FieldSchema("t", input, DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);
        
    }
    
}
