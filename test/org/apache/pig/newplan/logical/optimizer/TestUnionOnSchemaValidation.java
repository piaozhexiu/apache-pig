/**
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

package org.apache.pig.newplan.logical.optimizer;

import static org.apache.pig.ExecType.LOCAL;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.Test;


public class TestUnionOnSchemaValidation {

    @Test
    public void testUnionSchemaValidation() throws IOException {
        PigServer pigServer = new PigServer(LOCAL);
        
        Data data = Storage.resetData(pigServer);
        data.set("input1", 
                tuple(1));
        data.set("input2", 
                tuple(2, 3));
        data.set("input3", 
                tuple(4, 5, 6));

        pigServer.registerQuery(
                "inp1 = LOAD 'input1' USING mock.Storage() AS (a:int);"+
                "inp2 = LOAD 'input2' USING mock.Storage() AS (a:int, b:int);"+
                "inp3 = LOAD 'input3' USING mock.Storage() AS (a:int, b:int, c:int);"+
                "unioned = UNION ONSCHEMA inp1, inp2, inp3;"+
                "x = FOREACH unioned GENERATE a,b,c;"+
                "y = FOREACH inp3 GENERATE a;"+
                "store x into 'out1' using mock.Storage;"+
                "store y into 'out2' using mock.Storage;");
        
        List<Tuple> list1 = data.get("out1");
        List<Tuple> list2 = data.get("out2");
        
        assertEquals(3, list1.size());
        assertTrue(list1.containsAll(Arrays.asList(
                tuple(1,null,null),
                tuple(2,3,null),
                tuple(4,5,6))));

        assertEquals(1, list2.size());
        assertEquals("(4)", list2.get(0).toString());

    }

}
