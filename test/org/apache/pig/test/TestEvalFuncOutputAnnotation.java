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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.builtin.Unique;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestEvalFuncOutputAnnotation {

    private int trackNextSchemaId = 0;
    private static final Pattern GENERATED_FIELDNAME = Pattern.compile("[\\.\\$]");

    private static abstract class BaseFunc<T> extends EvalFunc<T> {
        @Override
        public T exec(Tuple input) throws IOException {
            return null;
        }
        
        //to be able to check unique fields id
        public static int getNextSchemaId() {
            return nextSchemaId;
        }
    }

    private static String getFuncNameAsFieldName(Class<?> clazz) {
        return GENERATED_FIELDNAME.matcher(clazz.getName().toLowerCase()).replaceAll("_");
    }

    @Before
    public void setUp() throws Exception {
        trackNextSchemaId = BaseFunc.getNextSchemaId();
    }

    @OutputSchema("")
    private static class Simple0 extends BaseFunc<String> {}

    @Test
    public void testSimple0() {
        EvalFunc<String> myFunc = new Simple0();
        assertNull(myFunc.outputSchema(null));
    }

    private static class Simple1 extends BaseFunc<Tuple> {
        @Override
        public Schema outputSchema(Schema input) {
            return new Schema(new FieldSchema("m", DataType.MAP));
        }
    }

    @Test
    public void testSimple1() throws FrontendException {
        EvalFunc<Tuple> myFunc = new Simple1();

        Schema expected = new Schema();
        expected.add(new FieldSchema("m", DataType.MAP));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {m: map[]}
    }

    @OutputSchema("")
    @Unique
    private static class Simple2 extends BaseFunc<String> {}

    @Test
    public void testSimple2() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple2();
        assertEquals(null, myFunc.outputSchema(null));
    }

    @Unique
    private static class Simple3 extends BaseFunc<String> {}

    @Test
    public void testSimple3() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple3();
        assertEquals(null, myFunc.outputSchema(null));
    }

    @Unique({})
    private static class Simple4 extends BaseFunc<String> {}

    @Test
    public void testSimple4() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple4();
        assertEquals(null, myFunc.outputSchema(null));
    }

    @OutputSchema("null")
    @Unique
    private static class Simple5 extends BaseFunc<String> {}

    @Test
    public void testSimple5() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple5();
        Schema expected = new Schema(new FieldSchema("null_" + trackNextSchemaId,
                DataType.BYTEARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {null_[nextSchemaId]: bytearray}
    }

    @OutputSchema("foo")
    private static class Simple6 extends BaseFunc<String> {}

    @Test
    public void testSimple6() {
        EvalFunc<String> myFunc = new Simple6();
        Schema s = new Schema(new FieldSchema("foo", DataType.BYTEARRAY));
        assertEquals(s, myFunc.outputSchema(null));
        //result: {foo: bytearray}
    }

    @OutputSchema("foo")
    @Unique
    private static class Simple7 extends BaseFunc<String> {}

    @Test
    public void testSimple7() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple7();
        Schema expected = new Schema(
                new FieldSchema("foo_" + trackNextSchemaId, DataType.BYTEARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {foo_[nextSchemaId]: bytearray}
    }

    @OutputSchema("chararray")
    private static class Simple8 extends BaseFunc<String> {}

    @Test
    public void testSimple8() {
        EvalFunc<String> myFunc = new Simple8();
        Schema s = new Schema(new FieldSchema(null, DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
    }

    @OutputSchema("chararray")
    @Unique
    private static class Simple9 extends BaseFunc<String> {}

    @Test
    public void testSimple9() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple9();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple9.class) + "_"
                + trackNextSchemaId, DataType.CHARARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple9_[nextSchemaId]: chararray}
    }

    @OutputSchema("foo:chararray")
    private static class Simple10 extends BaseFunc<String> {}

    @Test
    public void testSimple10() {
        EvalFunc<String> myFunc = new Simple10();
        Schema s = new Schema(new FieldSchema("foo", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
        //result: {foo: chararray}
    }

    @OutputSchema("foo:chararray")
    @Unique({ "bar" })
    private static class Simple11 extends BaseFunc<String> {}

    @Test
    public void testSimple11() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple11();
        Schema s = new Schema(new FieldSchema("foo", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
        //result: {foo: chararray}
    }
    
    @OutputSchema("foo:chararray")
    @Unique()
    private static class Simple12 extends BaseFunc<String> {}

    @Test
    public void testSimple12() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple12();
        Schema expected = new Schema(
                new FieldSchema("foo_" + trackNextSchemaId, DataType.CHARARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {foo_[nextSchemaId]: chararray}
    }

    @OutputSchema(value = "foo:chararray", useInputSchema = true)
    private static class Simple13 extends BaseFunc<String> {}

    @Test
    public void testSimple13() throws FrontendException {
        EvalFunc<String> myFunc = new Simple13();

        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        input.add(new FieldSchema(null, t, DataType.TUPLE));

        Schema s = new Schema(new FieldSchema("foo", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(input));
        //result: {foo: chararray}
    }

    @OutputSchema("t:tuple")
    @Unique
    private static class Simple14 extends BaseFunc<Tuple> {}

    @Test
    public void testSimple14() {
        trackNextSchemaId++;
        EvalFunc<Tuple> myFunc = new Simple14();
        Schema expected = new Schema(new FieldSchema("t_" + trackNextSchemaId, DataType.TUPLE));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {t_[nextSchemaId]: ()}
    }
 
    
    @OutputSchema(value = "t:()", useInputSchema = true)
    @Unique
    private static class Simple15 extends BaseFunc<Tuple> {}

    @Test
    public void testSimple15() throws FrontendException {
        trackNextSchemaId++;

        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        input.add(new FieldSchema(null, t, DataType.TUPLE));

        EvalFunc<Tuple> myFunc = new Simple15();

        Schema expected = new Schema(new FieldSchema("t_" + trackNextSchemaId, input,
                DataType.TUPLE));
        assertEquals(expected, myFunc.outputSchema(input));
        //result: {t_[nextSchemaId]: (org_apache_pig_test_testevalfuncoutputannotation_simple15_[nextSchemaId]: (i_[nextSchemaId]: int))}
    }

    @OutputSchema(value = "${0}:tuple", useInputSchema = true)
    @Unique({ "${0}" })
    private static class Simple16 extends BaseFunc<Tuple> {}

    @Test
    public void testSimple16() {
        trackNextSchemaId++;
        EvalFunc<Tuple> myFunc = new Simple16();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple16.class) + "_"
                + trackNextSchemaId, DataType.TUPLE));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation2_simple16_[nextSchemaId]: ()}
    }

    @OutputSchema(value = "${0}:tuple", useInputSchema = true)
    @Unique("${0}")
    private static class Simple17 extends BaseFunc<Tuple> {}

    @Test
    public void testSimple17() throws FrontendException {
        trackNextSchemaId++;
        EvalFunc<Tuple> myFunc = new Simple17();
        
        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        input.add(new FieldSchema(null, t, DataType.TUPLE));
        
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple17.class) + "_"
                + trackNextSchemaId, input, DataType.TUPLE));
        assertEquals(expected, myFunc.outputSchema(input));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple17_[nextSchemaId]: ((i: int))}
    }
    
    @OutputSchema(value = "${0}:map")
    @Unique
    private static class Simple18 extends BaseFunc<Map> {}

    @Test
    public void testSimple18() {
        trackNextSchemaId++;
        EvalFunc<Map> myFunc = new Simple18();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple18.class) + "_"
                + trackNextSchemaId, DataType.MAP));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation2_simple18_[nextSchemaId]: map[]}
    }

    @OutputSchema(value = "bag", useInputSchema = true)
    @Unique
    private static class Simple19 extends BaseFunc<DataBag> {}

    @Test
    public void testSimple19() throws FrontendException {
        trackNextSchemaId++;
        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        t.add(new FieldSchema("d", DataType.DOUBLE));
        input.add(new FieldSchema(null, t, DataType.BAG));

        EvalFunc<DataBag> myFunc = new Simple19();

        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple19.class) + "_" + trackNextSchemaId, input, DataType.BAG));
        assertEquals(expected, myFunc.outputSchema(input));
        /*
        result: 
        {org_apache_pig_test_testevalfuncoutputannotation_simple19_[nextSchemaId]: {
          org_apache_pig_test_testevalfuncoutputannotation_simple19_[nextSchemaId]: {
            i_[nextSchemaId]: int,d_[nextSchemaId]: double}}}
        */
    }

    @OutputSchema(value = "bag")
    @Unique
    private static class Simple20 extends BaseFunc<Tuple> {
        @Override
        public Schema outputSchema(Schema input) {
            Schema result = new Schema();
            try {
                result = new Schema(new FieldSchema("t", input.getField(1).schema, DataType.TUPLE));
            }
            catch (FrontendException e) {}
            return result;
        }
    }

    @Test
    public void testSimple20() throws FrontendException {
        EvalFunc<Tuple> myFunc = new Simple20();

        Schema input = new Schema();

        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        t.add(new FieldSchema("d", DataType.DOUBLE));
        input.add(new FieldSchema("c", DataType.CHARARRAY));
        input.add(new FieldSchema(null, t, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("t", t, DataType.TUPLE));

        assertEquals(expected, myFunc.outputSchema(input));
        //result: {t: (i: int,d: double)}
    }

    @OutputSchema("${0}")
    @Unique
    private static class Simple21 extends BaseFunc<String> {}

    @Test
    public void testSimple21() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple21();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple21.class) + "_"
                + trackNextSchemaId, DataType.BYTEARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple21_[nextSchemaId]: bytearray}
    }

    @OutputSchema("${0}")
    @Unique({})
    private static class Simple22 extends BaseFunc<String> {}

    @Test
    public void testSimple22() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple22();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple22.class) + "_"
                + trackNextSchemaId, DataType.BYTEARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple22_[nextSchemaId]: bytearray}
    }

    @OutputSchema("${0}")
    @Unique({ "${0}" })
    private static class Simple23 extends BaseFunc<String> {}

    @Test
    public void testSimple23() {
        trackNextSchemaId++;
        EvalFunc<String> myFunc = new Simple23();
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple23.class) + "_"
                + trackNextSchemaId, DataType.BYTEARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple23_[nextSchemaId]: bytearray}
    }
    
    @OutputSchema(value = "()", useInputSchema = true)
    @Unique
    private static class Simple24 extends BaseFunc<Tuple> {}
    
    @Test
    public void testSimple24() throws FrontendException {
        trackNextSchemaId++;

        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        input.add(new FieldSchema("inner", t, DataType.TUPLE));

        EvalFunc<Tuple> myFunc = new Simple24();

        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple24.class)
                + "_inner" + "_" + trackNextSchemaId, input, DataType.TUPLE));
        assertEquals(expected, myFunc.outputSchema(input));
        //result: {org_apache_pig_test_testevalfuncoutputannotation_simple24_inner_[nextSchemaId]: (inner_[nextSchemaId]: (i_[nextSchemaId]: int))}
    }
    
    @OutputSchema(value = "{()}", useInputSchema = true)
    @Unique
    private static class Simple25 extends BaseFunc<Tuple> {}
    
    @Test
    public void testSimple25() throws FrontendException {
        trackNextSchemaId++;

        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        input.add(new FieldSchema("inner", t, DataType.TUPLE));

        EvalFunc<Tuple> myFunc = new Simple25();
        int bagId = trackNextSchemaId++;
        Schema expectedTuple = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple25.class)
                + "_inner" + "_" + trackNextSchemaId, null, DataType.TUPLE));
        
        Schema expected = new Schema(new FieldSchema(getFuncNameAsFieldName(Simple25.class)
                + "_inner" + "_" + bagId, expectedTuple, DataType.BAG));
        assertEquals(expected, myFunc.outputSchema(input));
        /*result: 
         {org_apache_pig_test_testevalfuncoutputannotation_simple25_inner_[nextSchemaId]: {
           org_apache_pig_test_testevalfuncoutputannotation_simple25_inner_[nextSchemaId]: ()}}
        */
    }
    
    // complex

    @OutputSchema("{(i:int)}")
    private static class Complex0 extends BaseFunc<DataBag> {}
    
    @Test
    public void testComplex0() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex0();

        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));

        Schema tt = new Schema();
        tt.add(new FieldSchema(null, t, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("bag_0", tt, DataType.BAG));

        assertEquals(expected, myFunc.outputSchema(null));
        //result: {bag_0: {(i: int)}}
    }

    @OutputSchema(value = "{(i:int)}", useInputSchema = true)
    private static class Complex1 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex1() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex1();

        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));

        Schema tt = new Schema();
        tt.add(new FieldSchema(null, t, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("bag_0", tt, DataType.BAG));

        assertEquals(expected, myFunc.outputSchema(null));
        //result: {bag_0: {(i: int)}}
    }

    @OutputSchema(value = "{  (i:   int)   }")
    private static class Complex2 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex2() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex2();

        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));

        Schema tt = new Schema();
        tt.add(new FieldSchema(null, t, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("bag_0", tt, DataType.BAG));

        assertEquals(expected, myFunc.outputSchema(null));
        //result: {bag_0: {(i: int)}}
    }

    @OutputSchema(value = "{(map[(i:int)])}")
    private static class Complex3 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex3() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex3();

        Schema f = new Schema();
        f.add(new FieldSchema("i", DataType.INTEGER));

        Schema t = new Schema();
        t.add(new FieldSchema(null, f, DataType.TUPLE));

        Schema m = new Schema();
        m.add(new FieldSchema("map_0", t, DataType.MAP));

        Schema tt = new Schema();
        tt.add(new FieldSchema(null, m, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("bag_0", tt, DataType.BAG));

        assertEquals(expected, myFunc.outputSchema(null));
        //result: {bag_0: {(map_0: map[(i: int)])}}
    }

    @OutputSchema(value = "word:chararray,word:chararray")
    @Unique 
    private static class Complex4 extends BaseFunc<DataBag> {}
    
    @Test
    public void testComplex4() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex4();
        trackNextSchemaId++;
        Schema expected = new Schema();
        expected.add(new FieldSchema("word_" + trackNextSchemaId, DataType.CHARARRAY));
        expected.add(new FieldSchema("word_" + (++trackNextSchemaId), DataType.CHARARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {word_[nextSchemaId]: chararray,word_[nextSchemaId]: chararray}
    }
    
    @OutputSchema(value = "word:chararray,word:chararray")
    @Unique("word")
    private static class Complex5 extends BaseFunc<DataBag> {}
    
    @Test
    public void testComplex5() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex5();
        trackNextSchemaId++;
        Schema expected = new Schema();
        expected.add(new FieldSchema("word_" + trackNextSchemaId, DataType.CHARARRAY));
        expected.add(new FieldSchema("word_" + (++trackNextSchemaId), DataType.CHARARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {word_[nextSchemaId]: chararray,word_[nextSchemaId]: chararray}
    }
    
    @OutputSchema(value = "word:chararray,word:chararray")
    @Unique({"word"})
    private static class Complex6 extends BaseFunc<DataBag> {}
    
    @Test
    public void testComplex6() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex6();
        trackNextSchemaId++;
        Schema expected = new Schema();
        expected.add(new FieldSchema("word_" + trackNextSchemaId, DataType.CHARARRAY));
        expected.add(new FieldSchema("word_" + (++trackNextSchemaId), DataType.CHARARRAY));
        assertEquals(expected, myFunc.outputSchema(null));
        //result: {word_[nextSchemaId]: chararray,word_[nextSchemaId]: chararray}
    }
    
    
    @OutputSchema(value = "{(map[(c:chararray, i:int,${0})])}")
    @Unique({ "${0}" })
    private static class Complex7 extends BaseFunc<DataBag> {
        @Override
        public Schema outputSchema(Schema input) {
            return null;
        }
    }

    @Test
    public void testComplex7() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex7();

        Schema input = new Schema();
        Schema t = new Schema();
        t.add(new FieldSchema("i", DataType.INTEGER));
        t.add(new FieldSchema("d", DataType.DOUBLE));
        input.add(new FieldSchema(null, t, DataType.BAG));

        assertEquals(null, myFunc.outputSchema(input));
    }

    @OutputSchema(value = "{(map[(c:chararray, i:int,${0})])}")
    @Unique({ "${0}" })
    private static class Complex8 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex8() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex8();
        trackNextSchemaId++;
        Schema f = new Schema();
        f.add(new FieldSchema("c", DataType.CHARARRAY));
        f.add(new FieldSchema("i", DataType.INTEGER));
        f.add(new FieldSchema(getFuncNameAsFieldName(Complex8.class) + "_" + trackNextSchemaId,
                DataType.BYTEARRAY));

        Schema t = new Schema();
        t.add(new FieldSchema(null, f, DataType.TUPLE));

        Schema m = new Schema();
        m.add(new FieldSchema("map_0", t, DataType.MAP));

        Schema tt = new Schema();
        tt.add(new FieldSchema(null, m, DataType.TUPLE));

        Schema expected = new Schema();
        expected.add(new FieldSchema("bag_0", tt, DataType.BAG));

        assertEquals(expected, myFunc.outputSchema(null));
        //result: {bag_0: {(map_0: map[(c: chararray,i: int,org_apache_pig_test_testevalfuncoutputannotation_complex8_[nextSchemaId]: bytearray)])}}
    }

    @OutputSchema("y:bag{t:tuple(len:int,${0}:int,word:chararray)},${1}:chararray,${2},word:chararray")
    @Unique({ "word", "${0}", "${1}", "${2}" })
    private static class Complex9 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex9() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex9();
        String unique = getFuncNameAsFieldName(Complex9.class);
        Schema expected = new Schema();

        Schema t = new Schema();
        t.add(new FieldSchema("len", DataType.INTEGER));
        t.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.INTEGER));
        t.add(new FieldSchema("word_" + (++trackNextSchemaId), DataType.CHARARRAY));

        Schema tt = new Schema();
        tt.add(new FieldSchema("t", t, DataType.TUPLE));

        expected.add(new FieldSchema("y", tt, DataType.BAG));
        expected.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.CHARARRAY));
        expected.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.BYTEARRAY));
        expected.add(new FieldSchema("word_" + (++trackNextSchemaId), DataType.CHARARRAY));

        assertEquals(expected, myFunc.outputSchema(null));
        /*result:
        {y: {t: (len: int,org_apache_pig_test_testevalfuncoutputannotation_complex9_[nextSchemaId]: int,word_[nextSchemaId]: chararray)},
            org_apache_pig_test_testevalfuncoutputannotation_complex9_[nextSchemaId]: chararray,
            org_apache_pig_test_testevalfuncoutputannotation_complex9_[nextSchemaId]: bytearray,word_[nextSchemaId]: chararray}
        */
    }

    @OutputSchema("y:bag{t:tuple(len:int,word:chararray,${0}:int)},${2}:chararray,${1}")
    @Unique({ "${2}", "${0}", "${1}", "word" })
    private static class Complex10 extends BaseFunc<DataBag> {}

    @Test
    public void testComplex10() throws FrontendException {
        EvalFunc<DataBag> myFunc = new Complex10();
        String unique = getFuncNameAsFieldName(Complex10.class);
        Schema expected = new Schema();

        Schema t = new Schema();
        t.add(new FieldSchema("len", DataType.INTEGER));
        t.add(new FieldSchema("word_" + ++trackNextSchemaId, DataType.CHARARRAY));
        t.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.INTEGER));

        Schema tt = new Schema();
        tt.add(new FieldSchema("t", t, DataType.TUPLE));

        expected.add(new FieldSchema("y", tt, DataType.BAG));
        expected.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.CHARARRAY));
        expected.add(new FieldSchema(unique + "_" + (++trackNextSchemaId), DataType.BYTEARRAY));

        assertEquals(expected, myFunc.outputSchema(null));
        /*
        result:
        {y: {t: (len: int,word_[nextSchemaId]: chararray,org_apache_pig_test_testevalfuncoutputannotation_complex10_[nextSchemaId]: int)},
            org_apache_pig_test_testevalfuncoutputannotation_complex10_[nextSchemaId]: chararray,
            org_apache_pig_test_testevalfuncoutputannotation_complex10_[nextSchemaId]: bytearray}
        */
    }
    
    @OutputSchema("${0}")
    private static class Fail0 extends BaseFunc<String> {}

    @Test
    public void testFail0() {
        trackNextSchemaId++;
        try {
            EvalFunc<String> myFunc = new Fail0();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Syntax error, unexpected symbol at or near '$'"));
        }
    }

    @OutputSchema("${0}:{${1}:(i:int,c:chararray)}")
    @Unique({ "${0}" })
    public class Fail1 extends BaseFunc<DataBag> {}

    @Test
    public void testFail1() {
        trackNextSchemaId++;
        try {
            EvalFunc<DataBag> myFunc = new Fail1();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Unable to resolve output schema placeholder: ${1}"));
        }
    }

    @OutputSchema("my${0}:(i:int,c:chararray)")
    @Unique({ "${0}" })
    public class Fail2 extends BaseFunc<Tuple> {}

    @Test
    public void testFail2() {
        trackNextSchemaId++;
        try {
            EvalFunc<Tuple> myFunc = new Fail2();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Invalid character before ${0} : y"));
        }
    }

    @OutputSchema("${my}:(i:int,c:chararray)")
    @Unique({ "${my}" })
    public class Fail3 extends BaseFunc<Tuple> {}

    @Test
    public void testFail3() {
        trackNextSchemaId++;
        try {
            EvalFunc<Tuple> myFunc = new Fail3();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(
                    "Output schema placeholder index should be numeric: ${my}"));
        }
    }

    @OutputSchema("${my}:(i:int,c:chararray)")
    @Unique({ "${}" })
    public class Fail4 extends BaseFunc<Tuple> {}

    @Test
    public void testFail4() {
        trackNextSchemaId++;
        try {
            EvalFunc<Tuple> myFunc = new Fail4();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage()
                    .contains("Unable to resolve output schema placeholder: ${my}"));
        }
    }

    @OutputSchema("${0}:(i:int,c:chararray)")
    @Unique({ "${0.12}" })
    public class Fail5 extends BaseFunc<Tuple> {}

    @Test
    public void testFail5() {
        trackNextSchemaId++;
        try {
            EvalFunc<Tuple> myFunc = new Fail5();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(
                    "Unable to resolve output schema placeholder: ${0}"));
        }
    }
    

    @OutputSchema("word:chararray,word:chararray")
    private static class Fail6 extends BaseFunc<DataBag> {}
    
    @Test
    public void testFail6() throws FrontendException {
        try {
            EvalFunc<DataBag> myFunc = new Fail6();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(
                    "Unable to create output schema. Duplicate schema alias: word"));
        }
    }
    
    @OutputSchema("num:double,y:bag{t:tuple(len:int,word:chararray,word:int)},word:chararray,num")
    @Unique({ "word" })
    private static class Fail7 extends BaseFunc<DataBag> {}

    @Test
    public void testFail7() throws FrontendException {
        try {
            EvalFunc<DataBag> myFunc = new Fail7();
            myFunc.outputSchema(null);
            fail();
        }
        catch (RuntimeException e) {
            assertTrue(e.getMessage().contains(
                    "Unable to create output schema. Duplicate schema alias: num"));
        }
    }

    // original tests
    
    @OutputSchema("foo:chararray")
    public static class AnnotatedFunc extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return null;
        }
    }

    @OutputSchema("foo:chararray")
    public static class OverriddenFunc extends EvalFunc<String> {
        @Override
        public String exec(Tuple input) throws IOException {
            return null;
        }
        @Override
        public Schema outputSchema(Schema input) {
            return new Schema(new FieldSchema("bar", DataType.CHARARRAY));
        }
    }

    // This would give the same result: "y:bag{tuple(len:int,word:chararray)}"
    @OutputSchema("y:bag{(len:int,word:chararray)}")
    public static class ComplexFunc extends EvalFunc<DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            return null;
        }
    }

    public static class UnannotatedFunc extends EvalFunc<DataBag> {
        @Override
        public DataBag exec(Tuple input) throws IOException {
            return null;
        }
    }

    @Test
    public void testSimpleAnnotation() {
        EvalFunc<String> myFunc = new AnnotatedFunc();
        Schema s = new Schema(new FieldSchema("foo", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
    }

    @Test
    public void testOverriddenAnnotation() {
        EvalFunc<String> myFunc = new OverriddenFunc();
        Schema s = new Schema(new FieldSchema("bar", DataType.CHARARRAY));
        assertEquals(s, myFunc.outputSchema(null));
        //result: {bar: chararray}
    }

    @Test
    public void testUnannotated() {
        EvalFunc<DataBag> myFunc = new UnannotatedFunc();
        assertNull(myFunc.outputSchema(null));
    }

    @Test
    public void testComplex() throws FrontendException {
        EvalFunc<DataBag> myFunc = new ComplexFunc();
        // y:bag{t:tuple(len:int,word:chararray)}
        Schema ts = new Schema(Lists.asList(new FieldSchema("len", DataType.INTEGER),
                new FieldSchema[] { new FieldSchema("word", DataType.CHARARRAY) }));
        // Pig silently drops the name of a tuple the bag hold, since it's more
        // or less invisible.
        FieldSchema bfs = new FieldSchema(null, ts, DataType.TUPLE);
        Schema bs = new Schema();
        bs.add(bfs);
        Schema s = new Schema();
        s.add(new FieldSchema("y", bs, DataType.BAG));
        assertEquals(s, myFunc.outputSchema(null));
        //result: {y: {(len: int,word: chararray)}}
    }
    
}
