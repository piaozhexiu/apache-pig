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

public class TestJythonUDFOutputAnnotation extends ScriptingUDFOutputAnnotationBase {

    private final ScriptingType type = ScriptingType.JYTHON;
    
    @Test
    public void testScript1() throws Exception {
        String scriptDef =
            "@outputSchema(\"my\")\n" +
            "def simple1(x):\n" +
            "    return x\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: int);",
                "B = FOREACH A GENERATE jythonudfs.simple1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.BYTEARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript2() throws Exception {
        String scriptDef =
            "@outputSchema(\"my:chararray\")\n" +
            "def simple2(x):\n" +
            "    return x\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE jythonudfs.simple2(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));

        Schema expected = new Schema(new Schema.FieldSchema("my", DataType.CHARARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript3() throws Exception {
        String scriptDef =
            "@outputSchema(\"chararray\")\n" +
            "def simple3(x):\n" +
            "    return x\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE jythonudfs.simple3(c);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple("foo"));

        Schema expected = new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
        execute(type, scriptDef, statements, "B", data, expected);

    }
    
    @Test
    public void testScript4() throws Exception {
        String scriptDef =
            "@outputSchema(value=\"t:tuple\", useInputSchema=True)\n" +
            "def test_tup1(word):\n" +
            "    return (word)\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE jythonudfs.test_tup1(c);"
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
            "@outputSchema(value=\"t:tuple(num:long,${0}:chararray)\")\n" +
            "@unique\n" +
            "def test_tup2(word):\n" +
            "    return (len(word), word)\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE jythonudfs.test_tup2(c);"
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
            "@outputSchema(value=\"t:tuple(num:long,${0}:chararray)\")\n" +
            "@unique([\"${0}\"])\n" +
            "def test_tup3(word):\n" +
            "    return (len(word), word)\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (c: chararray);",
                "B = FOREACH A GENERATE jythonudfs.test_tup3(c);"
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
            "@outputSchemaFunction(\"testfunc1schema\")\n" +
            "def testfunc1(x):\n" +
            "    return x*x\n" + 
            ""+
            "@schemaFunction(\"testfunc1schema\")\n"+
            "def testfunc1schema(input):\n"+
            "    s = Schema()\n"+
            "    s.add(Schema.FieldSchema(\"x\", DataType.LONG))\n" +
            "    return s\n";

        String[] statements = {
                "A = LOAD 'data' USING mock.Storage() AS (i: long);",
                "B = FOREACH A GENERATE jythonudfs.testfunc1(i);"
        };
    
        Data data = resetData(pigServer);
        data.set("data", tuple(1L));

        Schema expected = new Schema(new Schema.FieldSchema("x", DataType.LONG));
        execute(type, scriptDef, statements, "B", data, expected);
        
    }
    
}
