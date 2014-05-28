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

package org.apache.pig.piggybank.test.evaluation;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.piggybank.evaluation.IsDouble;
import org.apache.pig.piggybank.evaluation.IsFloat;
import org.apache.pig.piggybank.evaluation.IsInt;
import org.apache.pig.piggybank.evaluation.IsLong;
import org.apache.pig.piggybank.evaluation.IsNumeric;
import org.apache.pig.piggybank.evaluation.datetime.DiffDate;
import org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO;
import org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix;
import org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISODaysBetween;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISOHoursBetween;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISOMinutesBetween;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISOMonthsBetween;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISOSecondsBetween;
import org.apache.pig.piggybank.evaluation.datetime.diff.ISOYearsBetween;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToDay;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToHour;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToMinute;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToMonth;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToSecond;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToWeek;
import org.apache.pig.piggybank.evaluation.datetime.truncate.ISOToYear;
import org.apache.pig.piggybank.evaluation.decode.Bin;
import org.apache.pig.piggybank.evaluation.decode.BinCond;
import org.apache.pig.piggybank.evaluation.decode.Decode;
import org.apache.pig.piggybank.evaluation.math.DoubleCopySign;
import org.apache.pig.piggybank.evaluation.math.DoubleGetExponent;
import org.apache.pig.piggybank.evaluation.math.DoubleMax;
import org.apache.pig.piggybank.evaluation.math.DoubleMin;
import org.apache.pig.piggybank.evaluation.math.DoubleNextAfter;
import org.apache.pig.piggybank.evaluation.math.DoubleNextup;
import org.apache.pig.piggybank.evaluation.math.DoubleSignum;
import org.apache.pig.piggybank.evaluation.math.DoubleUlp;
import org.apache.pig.piggybank.evaluation.math.FloatCopySign;
import org.apache.pig.piggybank.evaluation.math.FloatGetExponent;
import org.apache.pig.piggybank.evaluation.math.FloatMax;
import org.apache.pig.piggybank.evaluation.math.FloatMin;
import org.apache.pig.piggybank.evaluation.math.FloatNextAfter;
import org.apache.pig.piggybank.evaluation.math.FloatNextup;
import org.apache.pig.piggybank.evaluation.math.FloatSignum;
import org.apache.pig.piggybank.evaluation.math.FloatUlp;
import org.apache.pig.piggybank.evaluation.math.IntMax;
import org.apache.pig.piggybank.evaluation.math.IntMin;
import org.apache.pig.piggybank.evaluation.math.LongMax;
import org.apache.pig.piggybank.evaluation.math.LongMin;
import org.apache.pig.piggybank.evaluation.math.MAX;
import org.apache.pig.piggybank.evaluation.math.MIN;
import org.apache.pig.piggybank.evaluation.math.NEXTUP;
import org.apache.pig.piggybank.evaluation.math.RANDOM;
import org.apache.pig.piggybank.evaluation.math.SCALB;
import org.apache.pig.piggybank.evaluation.math.SIGNUM;
import org.apache.pig.piggybank.evaluation.math.ULP;
import org.apache.pig.piggybank.evaluation.math.copySign;
import org.apache.pig.piggybank.evaluation.math.getExponent;
import org.apache.pig.piggybank.evaluation.math.nextAfter;
import org.apache.pig.piggybank.evaluation.string.HashFNV;
import org.apache.pig.piggybank.evaluation.string.LENGTH;
import org.apache.pig.piggybank.evaluation.string.LookupInFiles;
import org.apache.pig.piggybank.evaluation.string.RegexMatch;
import org.apache.pig.piggybank.evaluation.string.Reverse;
import org.apache.pig.piggybank.evaluation.string.Stuff;
import org.apache.pig.piggybank.evaluation.util.SearchQuery;
import org.apache.pig.test.utils.OutputAnnotationBase;
import org.junit.Before;
import org.junit.Test;

public class TestEvalOutputAnnotation extends OutputAnnotationBase {

    @Before
    public void setUp() throws Exception {
        trackNextSchemaId = getEvalFuncNextSchemaId();
    }
    
    @Test
    public void testIsDouble() throws FrontendException {
        IsDouble func = new IsDouble();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
   
    @Test
    public void testIsFloat() throws FrontendException {
        IsFloat func = new IsFloat();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testIsInt() throws FrontendException {
        IsInt func = new IsInt();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testIsLong() throws FrontendException {
        IsLong func = new IsLong();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testIsNumeric() throws FrontendException {
        IsNumeric func = new IsNumeric();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testDiffDate() throws FrontendException {
        DiffDate func = new DiffDate();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testCustomFormatToISO() throws FrontendException {
        CustomFormatToISO func = new CustomFormatToISO();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToUnix() throws FrontendException {
        ISOToUnix func = new ISOToUnix();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testUnixToISO() throws FrontendException {
        UnixToISO func = new UnixToISO();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISODaysBetween() throws FrontendException {
        ISODaysBetween func = new ISODaysBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOHoursBetween() throws FrontendException {
        ISOHoursBetween func = new ISOHoursBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOMinutesBetween() throws FrontendException {
        ISOMinutesBetween func = new ISOMinutesBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOMonthsBetween() throws FrontendException {
        ISOMonthsBetween func = new ISOMonthsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOSecondsBetween() throws FrontendException {
        ISOSecondsBetween func = new ISOSecondsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOYearsBetween() throws FrontendException {
        ISOYearsBetween func = new ISOYearsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testISOToDay() throws FrontendException {
        ISOToDay func = new ISOToDay();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToHour() throws FrontendException {
        ISOToHour func = new ISOToHour();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToMinute() throws FrontendException {
        ISOToMinute func = new ISOToMinute();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToMonth() throws FrontendException {
        ISOToMonth func = new ISOToMonth();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }

    @Test
    public void testISOToSecond() throws FrontendException {
        ISOToSecond func = new ISOToSecond();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToWeek() throws FrontendException {
        ISOToWeek func = new ISOToWeek();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testISOToYear() throws FrontendException {
        ISOToYear func = new ISOToYear();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testBin() throws FrontendException {
        Bin func = new Bin();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testBinCond() throws FrontendException {
        BinCond func = new BinCond();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testDecode() throws FrontendException {
        Decode func = new Decode();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testCopySign() throws FrontendException {
        copySign func = new copySign();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleCopySign() throws FrontendException {
        DoubleCopySign func = new DoubleCopySign();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleGetExponent() throws FrontendException {
        DoubleGetExponent func = new DoubleGetExponent();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testDoubleMax() throws FrontendException {
        DoubleMax func = new DoubleMax();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleMin() throws FrontendException {
        DoubleMin func = new DoubleMin();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleNextAfter() throws FrontendException {
        DoubleNextAfter func = new DoubleNextAfter();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleNextup() throws FrontendException {
        DoubleNextup func = new DoubleNextup();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleSignum() throws FrontendException {
        DoubleSignum func = new DoubleSignum();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testDoubleUlp() throws FrontendException {
        DoubleUlp func = new DoubleUlp();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testFloatCopySign() throws FrontendException {
        FloatCopySign func = new FloatCopySign();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatGetExponent() throws FrontendException {
        FloatGetExponent func = new FloatGetExponent();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testFloatMax() throws FrontendException {
        FloatMax func = new FloatMax();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatMin() throws FrontendException {
        FloatMin func = new FloatMin();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatNextAfter() throws FrontendException {
        FloatNextAfter func = new FloatNextAfter();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatNextup() throws FrontendException {
        FloatNextup func = new FloatNextup();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatSignum() throws FrontendException {
        FloatSignum func = new FloatSignum();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testFloatUlp() throws FrontendException {
        FloatUlp func = new FloatUlp();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }

    @Test
    public void testGetExponent() throws FrontendException {
        getExponent func = new getExponent();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testIntMax() throws FrontendException {
        IntMax func = new IntMax();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testIntMin() throws FrontendException {
        IntMin func = new IntMin();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testLongMax() throws FrontendException {
        LongMax func = new LongMax();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testLongMin() throws FrontendException {
        LongMin func = new LongMin();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testMAX() throws FrontendException {
        MAX func = new MAX();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testMIN() throws FrontendException {
        MIN func = new MIN();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testNextAfter() throws FrontendException {
        nextAfter func = new nextAfter();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testNextUp() throws FrontendException {
        NEXTUP func = new NEXTUP();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testRANDOM() throws FrontendException {
        RANDOM func = new RANDOM();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
 
    @Test
    public void testSCALB() throws FrontendException {
        SCALB func = new SCALB();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testSIGNUM() throws FrontendException {
        SIGNUM func = new SIGNUM();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testULP() throws FrontendException {
        ULP func = new ULP();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testHashFNV() throws FrontendException {
        HashFNV func = new HashFNV();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testLENGTH() throws FrontendException {
        LENGTH func = new LENGTH();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.INTEGER));
    }
    
    @Test
    public void testLookupInFiles() throws FrontendException {
        LookupInFiles func = new LookupInFiles();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testRegexMatch() throws FrontendException {
        RegexMatch func = new RegexMatch();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testReverse() throws FrontendException {
        Reverse func = new Reverse();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testStuff() throws FrontendException {
        Stuff func = new Stuff();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testSearchQuery() throws FrontendException {
        SearchQuery func = new SearchQuery();
        testRunner(func, getOutputSchema(func, "query", null, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
}
