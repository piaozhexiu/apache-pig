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

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Before;

public abstract class ScriptingUDFOutputAnnotationBase {

    protected int trackNextSchemaId = 0;
    private static Field field_nextSchemaId;

    // need to access the internal field nextSchemaId to be able to test
    // unique fieldname generation
    static {
        try {
            field_nextSchemaId = EvalFunc.class.getDeclaredField("nextSchemaId");
            field_nextSchemaId.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public int getEvalFuncNextSchemaId() {
        try {
            return field_nextSchemaId.getInt(null);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
  
    public enum ScriptingType {
        GROOVY("groovy", "groovyudfs", ".groovy") {
            @Override
            public String getImportStatement() {
                return
                "import org.apache.pig.builtin.OutputSchema;\n" +
                "import org.apache.pig.builtin.Unique;\n" +
                "import org.apache.pig.scripting.groovy.OutputSchemaFunction;\n" +
                "import org.apache.pig.data.Tuple;\n" +
                "import org.apache.pig.data.DataBag;\n" +
                "import org.apache.pig.data.DataType;\n" +
                "import org.apache.pig.builtin.mock.Storage;\n" +
                "import org.apache.pig.impl.logicalLayer.schema.Schema;\n" +
                "";
            }
        },
        JYTHON("jython", "jythonudfs", ".py") {
            @Override
            public String getImportStatement() {
                return
                "#!/usr/bin/python\n" +
                "from org.apache.pig.scripting import Pig\n" +
                "from org.apache.pig.impl.logicalLayer.schema import Schema\n" + 
                "from org.apache.pig.data import DataType\n" +
                "";
            }
        }, 
        PYTHON("streaming_python", "pythonudfs", ".py") {
            @Override
            public String getImportStatement() {
                return
                "#!/usr/bin/python\n" +
                "from pig_util import outputSchema, unique\n" +
                "";
            }
        },
        JRUBY("jruby", "jrubyudfs", ".rb") {
            @Override
            public String getImportStatement() {
                String baseDir = "file://" + System.getProperty("user.dir");
                String pigudfPath = "/pig-withouthadoop.jar!/pigudf.rb";
                return "require '" + baseDir + pigudfPath + "'";
            }
        };

        private String scriptingLang;
        private String namespace;
        private String suffix;

        private ScriptingType(String scriptingLang, String namespace, String suffix) {
            this.scriptingLang = scriptingLang;
            this.namespace = namespace;
            this.suffix = suffix;
        }
        
        public abstract String getImportStatement();

        public String getScriptingLang() {
            return scriptingLang;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getSuffix() {
            return suffix;
        }
    }
    
    protected PigServer pigServer;
    
    @Before
    public void setUp() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL, new Properties());
    }
   
    /**
     * Base method for executing testcases
     * 
     * @param type - scripting type
     * @param scriptDef script content
     * @param statements pig statements
     * @param dumpAlias which alias to check
     * @param data input data
     * @param expected output schema to be expected
     * @throws Exception
     */
    public void execute(ScriptingType type, String scriptDef, String[] statements,
            String dumpAlias, Data data, Schema expected) throws Exception {
        String fileName = "temp_" + type.name().toLowerCase() + "_udf";
        String[] pythonStatements = { type.getImportStatement() + "\n" + scriptDef };
        File tmpScriptFile = File.createTempFile(fileName, type.getSuffix());
        tmpScriptFile.deleteOnExit();
        FileWriter writer = new FileWriter(tmpScriptFile);
        for (String line : pythonStatements) {
            writer.write(line + "\n");
        }
        writer.close();

        pigServer.registerCode(tmpScriptFile.getCanonicalPath(), type.getScriptingLang(),
                type.getNamespace());
        for (String stmt : statements) {
            pigServer.registerQuery(stmt);
        }

        Schema actual = pigServer.dumpSchema(dumpAlias);
        assertEquals(expected, actual);
        
    }
   
  
}
