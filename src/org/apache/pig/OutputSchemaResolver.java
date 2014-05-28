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
package org.apache.pig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.builtin.Unique;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;
import org.python.google.common.collect.Sets;

/**
 * This class is responsible for interpreting and parsing the UDF output schema definition given by 
 * the {@link OutputSchema @OutputSchema} and {@link Unique @Unique} annotations.
 *
 */
public class OutputSchemaResolver {

    private static final Pattern SCHEMA_PLACEHOLDER = Pattern.compile("\\$\\{(.+?)\\}");
    private static final Pattern GENERATED_FIELDNAME = Pattern.compile("[\\.\\$]");
    private static final Pattern SCHEMA_TRIM = Pattern.compile("\\s+");
    private static final Pattern SIMPLE_SCHEMA = Pattern.compile(
            "(\\w+)(:(\\w+|\\{\\}|\\(\\)|\\[\\]))?", Pattern.CASE_INSENSITIVE);

    /** unique udf column id, see {@link EvalFunc#getSchemaName(String, Schema)} */
    private int nextSchemaId;

    private final Schema input;
    private final String schema;
    private final boolean useInputSchema;

    /** placeholders take the name of the operating class */
    private final String funcNameAsFieldName;
    
    /**
     * Flag that indicates whether all fields in the schema (except those, taken from the inputSchema)
     * must be unique. This is set if {@link Unique @Unique} has no field enumeration
     */
    private final boolean allUnique;

    /** unique fields name taken from {@link Unique @Unique} */
    private final Set<String> uniqueFields;

    /**
     * @param input - input schema 
     * @param functionName - name of the function being called
     * @param schemaDef - schema String to be parsed
     * @param uniqueFieldsDef - array of field names to be made unique
     * @param useInputSchema - if true simple schemas will incorporate the input schema
     * @param nextSchemaId - unique field id
     */
    public OutputSchemaResolver(Schema input, String functionName, String schemaDef,
            String[] uniqueFieldsDef, boolean useInputSchema, int nextSchemaId) {
        this.input = input;
        this.funcNameAsFieldName = getFuncNameAsFieldName(functionName);
        this.schema = (schemaDef == null) ? null : SCHEMA_TRIM.matcher(schemaDef).replaceAll("");
        this.allUnique = (uniqueFieldsDef == null) ? false : uniqueFieldsDef.length == 0;
        this.uniqueFields = (uniqueFieldsDef == null) ? Collections.<String> emptySet() : 
            Sets.newHashSet(Arrays.asList(uniqueFieldsDef));
        this.useInputSchema = useInputSchema;
        this.nextSchemaId = nextSchemaId;
    }
    
    /**
     * @param functionName - name of the function being called
     * @param schema - schema String to be parsed
     * @param uniqueFieldsDef - array of field names to be made unique
     */
    public OutputSchemaResolver(String functionName, String schema,
            String[] uniqueFieldsDef) {
        this(null, functionName, schema, uniqueFieldsDef, false, 0);
    }
    
    /**
     * Placeholder fields are substituted with the name of the class.
     * Characters '.' and '$' are replaced with '_'.
     * 
     * @param funcName
     * @return
     */
    private String getFuncNameAsFieldName(String funcName) {
        if (funcName == null)
            return null;
        funcName = GENERATED_FIELDNAME.matcher(funcName).replaceAll("_");
        return getSchemaAlias(funcName, input);
    }
    
    /**
     * Creates a unique UDF column name
     * 
     * @param name - name of the field
     * @param input - input schema
     * @param nextSchemaId - unique id of the UDF column
     * @return a unqiue coloumn name
     */
    public static String getSchemaName(String name, Schema input, int nextSchemaId) {
        return getSchemaAlias(name, input) + "_" + nextSchemaId;
    }
    
    private static String getSchemaAlias(String name, Schema input) {
        if (input!=null && input.getAliases().size() > 0){
            name += "_" + input.getAliases().iterator().next();
        }
        return name;
    }
    
    /**
     * EvalFunc updates its internal unique id after the output schema is generated.
     * @return
     */
    public int getUpdatedNextSchemaId() {
        return nextSchemaId;
    }
 
    /**
     * Returns the output schema of the UDF
     * @return Schema of the output
     * @throws ParserException 
     */
    public Schema resolveSchema() throws ParserException {
        Schema result = null;
        if (schema == null) {
            return null;
        }
        String substitutedSchema = resolvePlaceholders();
        
        //try to resolve first as a simple schema
        result = asSimpleSchema(substitutedSchema);

        //resolve it as a complex schema
        if (result == null) {
            // during parsing, duplicate field checking is temporarily disabled
            result = Utils.getSchemaFromString(substitutedSchema, false, !allUnique);
        }
        
        //walk through the schema and uniquify fields
        adjustSchema(result);
        
        //check if there are duplicates
        checkDuplicateSchemaAlias(result);
        return result;
    }
    
    /**
     * Resolves the schema definition as a simple schema.
     * Schemas are considered simple if they are in form of <code>'[fielname] [-fieldtye]'</code>
     * 
     * @param substitutedSchema - schema to be resolved
     * @return
     */
    private Schema asSimpleSchema(String substitutedSchema) {
        
        String fieldName = null;
        byte fieldType = DataType.BYTEARRAY;
        
        //if schema is just a complex type abbreviation, e.g: (), {}
        byte abbrType = findDataType(substitutedSchema);
        if (abbrType != DataType.NULL && abbrType != DataType.UNKNOWN) {
            fieldType = abbrType;
        }
        else {
            if (!SIMPLE_SCHEMA.matcher(substitutedSchema).matches())
                return null; // complex schema, handled differently
            
            String[] parts = substitutedSchema.split(":");
            if (parts.length == 1) {
                byte type = findDataType(parts[0]);
                if (type == DataType.UNKNOWN)
                    fieldName = parts[0];
                else
                    fieldType = type;
            }
            else if (parts.length == 2) {
                fieldName = parts[0];
                byte type = findDataType(parts[1]);
                if (type == DataType.UNKNOWN)
                    new ParserException("Annotated schema definition is invalid!");
                fieldType = type;
            }
            else
                return null; // complex schema, handled differently
        }
        
        try {
            return new Schema(new Schema.FieldSchema(fieldName,
                    ((useInputSchema && DataType.isComplex(fieldType)) ? input : null), fieldType));
        }
        catch (FrontendException e) {
            throw new RuntimeException("Unable to create output schema field", e);
        }
    }

    /**
     * Makes fields unique in the parsed schema
     * @param schema - schema to be adjusted
     */
    private void adjustSchema(Schema schema) {
        try {
            for (int i = 0; i < schema.size(); i++) {
                FieldSchema fs = schema.getField(i);
                fs.alias = uniquify(fs.alias);
                if (fs.schema != null
                        && (fs.type == DataType.TUPLE || fs.type == DataType.BAG || fs.type == DataType.MAP)) {
                    adjustSchema(fs.schema);
                }
            }
        }
        catch (FrontendException e) {
            throw new RuntimeException("Unable to get outputschema field", e);
        }
    }

    /**
     * Validates and resolves placeholders in the schema definition.
     * @return substituted schema string
     */
    private String resolvePlaceholders() {
        if (!allUnique && uniqueFields.isEmpty()) {
            return schema;
        }
        Matcher m = SCHEMA_PLACEHOLDER.matcher(schema);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String placeholder = m.group(0);
            if (!allUnique && !uniqueFields.contains(placeholder)) {
                throw new RuntimeException("Unable to resolve output schema placeholder: "
                        + placeholder);
            }
            String idx = m.group(1);
            checkPlaceholder(placeholder, idx, m.start(), m.end());
            m.appendReplacement(sb, funcNameAsFieldName);
        }
        m.appendTail(sb);
        return sb.toString();
    }
    
    /**
     * Makes a field unique within the schema, if fieldname:
     * <pre>
     * - is a placeholder or
     * - defined in the @Unique annotation or
     * - allUnique is set
     * </pre>
     * @param fieldName
     * @return
     */
    private String uniquify(String fieldName) {
        if (allUnique || funcNameAsFieldName.equals(fieldName) || uniqueFields.contains(fieldName))
            return (fieldName == null ? funcNameAsFieldName : fieldName) + "_" + ++nextSchemaId;
        if (fieldName == null)
            return null;
        return fieldName;
    }
    
    /**
     * Checks whether a placeholder is valid.
     * Rules:
     * <pre>
     * - must be in form of ${i} where i=0..n
     * - cannot be combined with field names, e.g: word${0}Id is invalid
     * </pre>
     * @param placeholder
     * @param idx
     * @param startMatchingPos
     * @param endMatchingPos
     */
    private void checkPlaceholder(String placeholder, String idx, int startMatchingPos, int endMatchingPos) {
        try {
            Integer.parseInt(idx);
        }
        catch (NumberFormatException e) {
            throw new RuntimeException(
                    "Output schema placeholder index should be numeric: " + placeholder);
        }
        // check if placeholders syntax is correct
        if (startMatchingPos != 0) {
            char c = schema.charAt(startMatchingPos - 1);
            if (!(c == '{' || c == '(' || c == ',')) {
                throw new RuntimeException(String.format("Invalid character before %s : %s",
                        placeholder, c));
            }
        }
        if (endMatchingPos != schema.length()) {
            char c = schema.charAt(endMatchingPos);
            if (!(c == ',' || c == ':' || c == ')' || c == '}')) {
                throw new RuntimeException(String.format("Invalid character after %s : %s",
                        placeholder, c));
            }
        }
    }
    
    /**
     * Duplicate schema alias validation is done here instead in the AST validator
     * because the parsed schema, when being uniquified may be in an internal state where
     * duplicate aliases may exist and we don't want AST validator to throw exception in this
     * case
     * @param schema - schema to be checked
     */
    private void checkDuplicateSchemaAlias(Schema schema) {
        if (schema == null)
            return;
        Set<String> seenAliases = new HashSet<String>();
        try {
            for (int i = 0; i < schema.size(); i++) {
                if (schema.getField(i) != null && schema.getField(i).alias != null) {
                    String alias = schema.getField(i).alias;
                    if (seenAliases.contains(alias)) {
                        String msg = "Unable to create output schema. Duplicate schema alias: "
                            + schema.getField(i).alias;
                        throw new RuntimeException(msg);
                    }
                    seenAliases.add(alias);
                }
            }
        }
        catch (FrontendException e) {
           throw new RuntimeException("Unable to check output schema for duplicate aliases", e);
        }
    }
    
    /**
     * Determines data type for a given input. Abbreviations (<code>(),{},[]</code>) are taken into account
     * @return byte DataType value
     */
    private byte findDataType(String input) {
        if ("()".equals(input)) return DataType.TUPLE;
        if ("{}".equals(input)) return DataType.BAG;
        if ("[]".equals(input)) return DataType.MAP;
        return DataType.findTypeByName(input);
    }
}