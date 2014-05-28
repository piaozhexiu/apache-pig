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

public class TestGroovyUDFOutputAnnotation extends ScriptingUDFOutputAnnotationBase {

    private final ScriptingType type = ScriptingType.GROOVY;
   
    @Test
    public void testScript1() throws Exception {
        String scriptDef =
            "class GroovyUDF {" +
            "  @OutputSchema('my')" +
            "   long simple1(long x) {" +
            "      return x;" +
            "  }" +
            "}";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE groovyudfs.simple1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.BYTEARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

  }
  
  @Test
  public void testScript2() throws Exception {
        String scriptDef =
                "class GroovyUDF {" +
                "  @OutputSchema('my:chararray')" +
                "  String simple2(String x) {" +
                "      return x;" +
                "  }" +
                "}";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE groovyudfs.simple2(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.CHARARRAY));
        execute(type, scriptDef, statements, "B", data, expected);
    
    }

    @Test
    public void testScript3() throws Exception {
        String scriptDef =
                "class GroovyUDF {" +
                "  @OutputSchema('chararray')" +
                "   String simple3(String x) {" +
                "      return x;" +
                "  }" +
                "}";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE groovyudfs.simple3(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));

        Schema expected = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
  
    @Test
    public void testScript4() throws Exception {
        String scriptDef =
                "class GroovyUDF {" +
                "  @OutputSchema(value='t:tuple', useInputSchema=true)" +
                "  Tuple test_tup1(String word) {" +
                "      Tuple t = Storage.tuple(word);  " +
                "      return t;" +
                "  }" +
                "}";
    
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE groovyudfs.test_tup1(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));

        Schema input = new Schema(new Schema.FieldSchema("c", DataType.CHARARRAY));
        Schema expected = new Schema(new Schema.FieldSchema("t", input, DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);

    }
  
    @Test
    public void testScript5() throws Exception {
        String scriptDef =
                "class GroovyUDF {" +
                "  @OutputSchema(value='t:tuple(num:long,${0}:chararray)')" +
                "  @Unique  " +
                "  Tuple test_tup2(String word) {" +
                "      Tuple t = Storage.tuple(word.length(), word);  " +
                "      return t;" +
                "  }" +
                "}";
    
        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE groovyudfs.test_tup2(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));
        int magic = 9;
        int currentSchemaId = getEvalFuncNextSchemaId() + magic;
        int inputSchemaId = ++currentSchemaId;
        Schema input = new Schema();
        input.add(new Schema.FieldSchema("num_" + ++inputSchemaId, DataType.LONG));
        input.add(new Schema.FieldSchema("test_tup2_c_" + ++inputSchemaId, DataType.CHARARRAY));

        Schema expected = new Schema(new Schema.FieldSchema("t_" + currentSchemaId, input,
                DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);

    }
  
    @Test
    public void testScript6() throws Exception {
        String scriptDef =
                "class GroovyUDF {" +
                "  @OutputSchema('t:tuple(num:long,${0}:chararray)')" +
                "  @Unique(['${0}'])  " +
                "  Tuple test_tup3(String word) {" +
                "      Tuple t = Storage.tuple(word.length(), word);  " +
                "      return t;" +
                "  }" +
                "}";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE groovyudfs.test_tup3(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));
        int magic = 3;
        int currentSchemaId = getEvalFuncNextSchemaId() + magic;
        Schema input = new Schema();
        input.add(new Schema.FieldSchema("num", DataType.LONG));
        input.add(new Schema.FieldSchema("test_tup3_c_" + ++currentSchemaId, DataType.CHARARRAY));

        Schema expected = new Schema(new Schema.FieldSchema("t", input, DataType.TUPLE));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript7() throws Exception {
        String scriptDef =
            "@OutputSchemaFunction('testfunc1schema')\n" + 
            "public static testfunc1(x) {\n" +
            "    return x * x;\n"+
            "}\n" +
            "\n"+
            "public static testfunc1schema(input) {\n"+          
            "    return input;\n"+
            "}\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: long);",
                "B = FOREACH A GENERATE groovyudfs.testfunc1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1L));
        
        Schema expected = new Schema(new Schema.FieldSchema("i", DataType.LONG));
        execute(type, scriptDef, statements, "B", data, expected);

    }
}
