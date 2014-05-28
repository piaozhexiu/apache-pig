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

package org.apache.pig.test.utils;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class OutputAnnotationBase {

    protected int trackNextSchemaId = 0;
    private static Field field_nextSchemaId;
    private static Method method_getSchemaName;
    private static final Pattern GENERATED_FIELDNAME = Pattern.compile("[\\.\\$]");

    // need to access the internal field nextSchemaId to be able to test
    // unique fieldname generation
    static {
        try {
            field_nextSchemaId = EvalFunc.class.getDeclaredField("nextSchemaId");
            field_nextSchemaId.setAccessible(true);
            method_getSchemaName = EvalFunc.class.getDeclaredMethod("getSchemaName", new Class[]{String.class, Schema.class});
            method_getSchemaName.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    protected void setBackNextSchemaId(int previous) {
        try {
            field_nextSchemaId.set(null, previous);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected int getEvalFuncNextSchemaId() {
        try {
            return field_nextSchemaId.getInt(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    protected Schema getOutputSchema(EvalFunc<?> func, SchemaType schemaType, byte dataType) {
        return getOutputSchema(func, null, null, schemaType, dataType);
    }
    
    protected Schema getOutputSchema(EvalFunc<?> func, String name, Schema input, SchemaType schemaType, byte dataType) {
        try {
            if (schemaType == SchemaType.DEFAULT)
                return new Schema(new Schema.FieldSchema(name, input, dataType));
            
            return new Schema(new Schema.FieldSchema(getSchemaName(func,
                    func.getClass().getName().toLowerCase(), null), input, dataType));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    protected String getSchemaName(EvalFunc<?> func, String name, Schema input) {
        try {
            String result = (String) method_getSchemaName.invoke(func, name, input);
            //also adjust returned schema name: '$' and '.' are replaced with '_'
            return GENERATED_FIELDNAME.matcher(result).replaceAll("_");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    protected static enum SchemaType {
        DEFAULT,UNIQUE;
    }
    
    public void testRunner(EvalFunc<?> func, Schema originalOutputSchema) {
        testRunner(func, null, originalOutputSchema);
    }

    public void testRunner(EvalFunc<?> func, Schema funcInputSchema, Schema originalOutputSchema) {
        try {
            setBackNextSchemaId(trackNextSchemaId);     
            assertEquals(originalOutputSchema, func.outputSchema(funcInputSchema));
        }
        catch (Exception e) {
            throw new RuntimeException("Couldn't instantiate test class", e);
        }
    }
}
