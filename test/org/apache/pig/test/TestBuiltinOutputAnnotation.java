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

import org.apache.pig.builtin.ABS;
import org.apache.pig.builtin.AVG;
import org.apache.pig.builtin.AddDuration;
import org.apache.pig.builtin.BagSize;
import org.apache.pig.builtin.BigDecimalAvg;
import org.apache.pig.builtin.BigIntegerAvg;
import org.apache.pig.builtin.BuildBloom;
import org.apache.pig.builtin.CONCAT;
import org.apache.pig.builtin.COR;
import org.apache.pig.builtin.COUNT;
import org.apache.pig.builtin.COUNT_STAR;
import org.apache.pig.builtin.COV;
import org.apache.pig.builtin.ConstantSize;
import org.apache.pig.builtin.CubeDimensions;
import org.apache.pig.builtin.DateTimeMax;
import org.apache.pig.builtin.DateTimeMin;
import org.apache.pig.builtin.DaysBetween;
import org.apache.pig.builtin.DoubleAvg;
import org.apache.pig.builtin.DoubleRound;
import org.apache.pig.builtin.ENDSWITH;
import org.apache.pig.builtin.FloatAbs;
import org.apache.pig.builtin.FloatAvg;
import org.apache.pig.builtin.FloatRound;
import org.apache.pig.builtin.GetDay;
import org.apache.pig.builtin.GetHour;
import org.apache.pig.builtin.GetMilliSecond;
import org.apache.pig.builtin.GetMinute;
import org.apache.pig.builtin.GetMonth;
import org.apache.pig.builtin.GetSecond;
import org.apache.pig.builtin.GetWeek;
import org.apache.pig.builtin.GetWeekYear;
import org.apache.pig.builtin.GetYear;
import org.apache.pig.builtin.HoursBetween;
import org.apache.pig.builtin.INDEXOF;
import org.apache.pig.builtin.IntAbs;
import org.apache.pig.builtin.IntAvg;
import org.apache.pig.builtin.LAST_INDEX_OF;
import org.apache.pig.builtin.LCFIRST;
import org.apache.pig.builtin.LOWER;
import org.apache.pig.builtin.LTRIM;
import org.apache.pig.builtin.LongAbs;
import org.apache.pig.builtin.LongAvg;
import org.apache.pig.builtin.MapSize;
import org.apache.pig.builtin.MilliSecondsBetween;
import org.apache.pig.builtin.MinutesBetween;
import org.apache.pig.builtin.MonthsBetween;
import org.apache.pig.builtin.RANDOM;
import org.apache.pig.builtin.REGEX_EXTRACT;
import org.apache.pig.builtin.REGEX_EXTRACT_ALL;
import org.apache.pig.builtin.REPLACE;
import org.apache.pig.builtin.ROUND;
import org.apache.pig.builtin.RTRIM;
import org.apache.pig.builtin.RollupDimensions;
import org.apache.pig.builtin.SIZE;
import org.apache.pig.builtin.STARTSWITH;
import org.apache.pig.builtin.STRSPLIT;
import org.apache.pig.builtin.SUBSTRING;
import org.apache.pig.builtin.SecondsBetween;
import org.apache.pig.builtin.StringConcat;
import org.apache.pig.builtin.StringMax;
import org.apache.pig.builtin.StringMin;
import org.apache.pig.builtin.StringSize;
import org.apache.pig.builtin.SubtractDuration;
import org.apache.pig.builtin.TOMAP;
import org.apache.pig.builtin.TOTUPLE;
import org.apache.pig.builtin.TRIM;
import org.apache.pig.builtin.ToDate;
import org.apache.pig.builtin.ToMilliSeconds;
import org.apache.pig.builtin.ToString;
import org.apache.pig.builtin.ToUnixTime;
import org.apache.pig.builtin.TupleSize;
import org.apache.pig.builtin.UCFIRST;
import org.apache.pig.builtin.UPPER;
import org.apache.pig.builtin.WeeksBetween;
import org.apache.pig.builtin.YearsBetween;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.test.utils.OutputAnnotationBase;
import org.junit.Before;
import org.junit.Test;

public class TestBuiltinOutputAnnotation extends OutputAnnotationBase {
    
    @Before
    public void setUp() throws Exception {
        trackNextSchemaId = getEvalFuncNextSchemaId();
    }
    
    @Test
    public void testGetYear() throws FrontendException {
        GetYear func = new GetYear();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testDateTimeMax() throws FrontendException {
        DateTimeMax func = new DateTimeMax();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DATETIME));
    }
    
    @Test
    public void testGetMilliSecond() throws FrontendException {
        GetMilliSecond func = new GetMilliSecond();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
   
    @Test
    public void testGetWeekYear() throws FrontendException {
        GetWeekYear func = new GetWeekYear();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testCOUNT_STAR() throws FrontendException {
        COUNT_STAR func = new COUNT_STAR();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testCOR() throws FrontendException {
        COR func = new COR();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.BAG));
    }
    
    @Test
    public void testTupleSize() throws FrontendException {
        TupleSize func = new TupleSize();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testCOUNT() throws FrontendException {
        COUNT func = new COUNT();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testFloatAbs() throws FrontendException {
        FloatAbs func = new FloatAbs();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.FLOAT));
    }
    
    @Test
    public void testAddDuration() throws FrontendException {
        AddDuration func = new AddDuration();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DATETIME));
    }
    
    @Test
    public void testMinutesBetween() throws FrontendException {
        MinutesBetween func = new MinutesBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testGetMinute() throws FrontendException {
        GetMinute func = new GetMinute();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testABS() throws FrontendException {
        ABS func = new ABS();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testLCFIRST() throws FrontendException {
        LCFIRST func = new LCFIRST();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testDaysBetween() throws FrontendException {
        DaysBetween func = new DaysBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testGetDay() throws FrontendException {
        GetDay func = new GetDay();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testBigDecimalAvg() throws FrontendException {
        BigDecimalAvg func = new BigDecimalAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BIGDECIMAL));
    }
    
    @Test
    public void testSUBSTRING() throws FrontendException {
        SUBSTRING func = new SUBSTRING();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testIntAbs() throws FrontendException {
        IntAbs func = new IntAbs();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testConstantSize() throws FrontendException {
        ConstantSize func = new ConstantSize();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testRTRIM() throws FrontendException {
        RTRIM func = new RTRIM();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testStringSize() throws FrontendException {
        StringSize func = new StringSize();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testToUnixTime() throws FrontendException {
        ToUnixTime func = new ToUnixTime();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testToString() throws FrontendException {
        ToString func = new ToString();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testSubtractDuration() throws FrontendException {
        SubtractDuration func = new SubtractDuration();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DATETIME));
    }
    
    @Test
    public void testREGEX_EXTRACT() throws FrontendException {
        REGEX_EXTRACT func = new REGEX_EXTRACT();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testCubeDimensions() throws FrontendException {
        CubeDimensions func = new CubeDimensions();
        Schema originalOutputSchema = 
            new Schema(new Schema.FieldSchema("dimensions", null, DataType.BAG));
        testRunner(func, originalOutputSchema);
    }
    
    @Test
    public void testDoubleAvg() throws FrontendException {
        DoubleAvg func = new DoubleAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DOUBLE));
    }
    
    @Test
    public void testStringMin() throws FrontendException {
        StringMin func = new StringMin();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testLongAbs() throws FrontendException {
        LongAbs func = new LongAbs();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testLTRIM() throws FrontendException {
        LTRIM func = new LTRIM();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testBuildBloom() throws FrontendException {
        BuildBloom func = new BuildBloom("jenkins", "1000", "0.01");
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BYTEARRAY));
    }
    
    @Test
    public void testStringConcat() throws FrontendException {
        StringConcat func = new StringConcat();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testRollupDimensions() throws FrontendException {
        RollupDimensions func = new RollupDimensions();
        Schema originalOutputSchema = 
            new Schema(new Schema.FieldSchema("dimensions", null, DataType.BAG));
        testRunner(func, originalOutputSchema);
    }
    
    @Test
    public void testSTARTSWITH() throws FrontendException {
        STARTSWITH func = new STARTSWITH();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testUCFIRST() throws FrontendException {
        UCFIRST func = new UCFIRST();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testSTRSPLIT() throws FrontendException {
        STRSPLIT func = new STRSPLIT();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.TUPLE));
    }
    
    @Test
    public void testBagSize() throws FrontendException {
        BagSize func = new BagSize();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testCOV() throws FrontendException {
        COV func = new COV();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.BAG));
    }
    
    @Test
    public void testWeeksBetween() throws FrontendException {
        WeeksBetween func = new WeeksBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testGetWeek() throws FrontendException {
        GetWeek func = new GetWeek();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testFloatAvg() throws FrontendException {
        FloatAvg func = new FloatAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DOUBLE));
    }
    
    @Test
    public void testTOTUPLE() throws FrontendException {
        TOTUPLE func = new TOTUPLE();

        Schema input = new Schema();
        input.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        input.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        Schema originalOutputSchema = null;
        try {
            Schema tupleSchema = new Schema();
            for (int i = 0; i < input.size(); ++i) {
                tupleSchema.add(input.getField(i));
            }

            originalOutputSchema = new Schema(new Schema.FieldSchema(getSchemaName(func, func
                    .getClass().getName().toLowerCase(), null), input, DataType.TUPLE));
        }
        catch (Exception e) {}
        testRunner(func, input, originalOutputSchema);
    }
    
    @Test
    public void testSecondsBetween() throws FrontendException {
        SecondsBetween func = new SecondsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testSIZE() throws FrontendException {
        SIZE func = new SIZE();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testGetSecond() throws FrontendException {
        GetSecond func = new GetSecond();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testStringMax() throws FrontendException {
        StringMax func = new StringMax();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testFloatRound() throws FrontendException {
        FloatRound func = new FloatRound();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testAVG() throws FrontendException {
        AVG func = new AVG();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DOUBLE));
    }
    
    @Test
    public void testBigIntegerAvg() throws FrontendException {
        BigIntegerAvg func = new BigIntegerAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BIGDECIMAL));
    }
    
    @Test
    public void testDoubleRound() throws FrontendException {
        DoubleRound func = new DoubleRound();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testIntAvg() throws FrontendException {
        IntAvg func = new IntAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DOUBLE));
    }
    
    @Test
    public void testDateTimeMin() throws FrontendException {
        DateTimeMin func = new DateTimeMin();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DATETIME));
    }
    
    @Test
    public void testHoursBetween() throws FrontendException {
        HoursBetween func = new HoursBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testTRIM() throws FrontendException {
        TRIM func = new TRIM();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testREPLACE() throws FrontendException {
        REPLACE func = new REPLACE();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.CHARARRAY));
    }
    
    @Test
    public void testGetHour() throws FrontendException {
        GetHour func = new GetHour();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testLAST_INDEX_OF() throws FrontendException {
        LAST_INDEX_OF func = new LAST_INDEX_OF();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.INTEGER));
    }
    
    @Test
    public void testToDate() throws FrontendException {
        ToDate func = new ToDate();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DATETIME));
    }

    @Test
    public void testMonthsBetween() throws FrontendException {
        MonthsBetween func = new MonthsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testENDSWITH() throws FrontendException {
        ENDSWITH func = new ENDSWITH();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BOOLEAN));
    }
    
    @Test
    public void testGetMonth() throws FrontendException {
        GetMonth func = new GetMonth();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.INTEGER));
    }
    
    @Test
    public void testToMilliSeconds() throws FrontendException {
        ToMilliSeconds func = new ToMilliSeconds();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testCONCAT() throws FrontendException {
        CONCAT func = new CONCAT();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.BYTEARRAY));
    }
    
    @Test
    public void testMapSize() throws FrontendException {
        MapSize func = new MapSize();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.LONG));
    }
    
    @Test
    public void testTOMAP() throws FrontendException {
        TOMAP func = new TOMAP();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.MAP));
    }
    
    @Test
    public void testRANDOM() throws FrontendException {
        RANDOM func = new RANDOM();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.DOUBLE));
    }
    
    @Test
    public void testROUND() throws FrontendException {
        ROUND func = new ROUND();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testLOWER() throws FrontendException {
        LOWER func = new LOWER();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testUPPER() throws FrontendException {
        UPPER func = new UPPER();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.CHARARRAY));
    }
    
    @Test
    public void testINDEXOF() throws FrontendException {
        INDEXOF func = new INDEXOF();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.INTEGER));
    }
    
    @Test
    public void testREGEX_EXTRACT_ALL() throws FrontendException {
        REGEX_EXTRACT_ALL func = new REGEX_EXTRACT_ALL();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.TUPLE));
    }
    
    @Test
    public void testLongAvg() throws FrontendException {
        LongAvg func = new LongAvg();
        testRunner(func, getOutputSchema(func, SchemaType.DEFAULT, DataType.DOUBLE));
    }
    
    @Test
    public void testYearsBetween() throws FrontendException {
        YearsBetween func = new YearsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    @Test
    public void testMilliSecondsBetween() throws FrontendException {
        MilliSecondsBetween func = new MilliSecondsBetween();
        testRunner(func, getOutputSchema(func, SchemaType.UNIQUE, DataType.LONG));
    }
    
    
}
