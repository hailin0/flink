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

package org.apache.flink.connector.jdbc.internal.converter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.IOException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.function.Function;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INSTANT_TYPE_INFO;

public class AnalyticDBRowConverter extends MySQLRowConverter {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Map<TypeInformation<?>, Integer> EXTENSION_TYPE_MAPPING = new HashMap<TypeInformation<?>, Integer>() {
        {
            put(INSTANT_TYPE_INFO, Types.TIMESTAMP);
        }
    };
    private static final Map<LogicalTypeRoot, Function<String, ArrayData>> ARRAY_DESERIALIZATION_CONVERTER = new HashMap() {
        {
            final TypeReference<List<String>> TYPE_LIST_STRING = new TypeReference<List<String>>(){};
            final TypeReference<List<Long>> TYPE_LIST_LONG = new TypeReference<List<Long>>(){};
            final TypeReference<List<Double>> TYPE_LIST_DOUBLE = new TypeReference<List<Double>>(){};
            final TypeReference<List<Boolean>> TYPE_LIST_BOOLEAN = new TypeReference<List<Boolean>>(){};

            put(LogicalTypeRoot.VARCHAR, (Function<String, ArrayData>) val -> {
                List<String> array = jsonParse(val, TYPE_LIST_STRING);
                return new GenericArrayData(toArray(array));
            });
            put(LogicalTypeRoot.BIGINT, (Function<String, ArrayData>) val -> {
                List<Long> array = jsonParse(val, TYPE_LIST_LONG);
                return new GenericArrayData(array.toArray());
            });
            put(LogicalTypeRoot.DOUBLE, (Function<String, ArrayData>) val -> {
                List<Double> array = jsonParse(val, TYPE_LIST_DOUBLE);
                return new GenericArrayData(array.toArray());
            });
            put(LogicalTypeRoot.BOOLEAN, (Function<String, ArrayData>) val -> {
                List<Boolean> array = jsonParse(val, TYPE_LIST_BOOLEAN);
                return new GenericArrayData(array.toArray());
            });
        }
    };
    private static final Map<LogicalTypeRoot, Function<ArrayData, String>> ARRAY_SERIALIZATION_CONVERTER = new HashMap() {
        {
            put(LogicalTypeRoot.VARCHAR, (Function<ArrayData, String>) val -> {
                List<String> array = new ArrayList<>();
                for (int i = 0; i < val.size(); i++) {
                    array.add(val.getString(i).toString());
                }
                return toJsonString(array);
            });
            put(LogicalTypeRoot.BIGINT, (Function<ArrayData, String>) val -> Arrays.toString(val.toLongArray()));
            put(LogicalTypeRoot.DOUBLE, (Function<ArrayData, String>) val -> Arrays.toString(val.toDoubleArray()));
            put(LogicalTypeRoot.BOOLEAN, (Function<ArrayData, String>) val -> Arrays.toString(val.toBooleanArray()));
        }
    };

    public AnalyticDBRowConverter(RowType rowType) {
        super(rowType);
    }

    @Override
    protected JdbcSerializationConverter createExternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return (val, index, statement) -> {
                    Timestamp timestamp = val.getTimestamp(index, getTimestampPrecision(type)).toTimestamp();
                    statement.setString(index, timestamp.toString());
                };
            case ARRAY:
                LogicalTypeRoot elementType = ((ArrayType) type).getElementType().getTypeRoot();
                return (val, index, statement) -> {
                    ArrayData arrayData = val.getArray(index);
                    String arrayToString = ARRAY_SERIALIZATION_CONVERTER.get(elementType).apply(arrayData);
                    statement.setString(index, arrayToString);
                };
            default:
                return super.createExternalConverter(type);
        }
    }

    @Override
    protected JdbcDeserializationConverter createInternalConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return val -> {
                    if (val instanceof LocalDateTime) {
                        return TimestampData.fromLocalDateTime((LocalDateTime)val);
                    } else if (val instanceof Timestamp) {
                        return TimestampData.fromTimestamp((Timestamp)val);
                    } else {
                        throw new IllegalArgumentException("unexpected type " + type + " convert class " + val.getClass());
                    }
                };
            case ARRAY:
                LogicalTypeRoot elementType = ((ArrayType) type).getElementType().getTypeRoot();
                return val -> ARRAY_DESERIALIZATION_CONVERTER.get(elementType).apply((String) val);
            default:
                return super.createInternalConverter(type);
        }
    }

    @Override
    protected JdbcSerializationConverter wrapIntoNullableExternalConverter(JdbcSerializationConverter jdbcSerializationConverter, LogicalType type) {
        int sqlType = getSqlType(type);
        return (val, index, statement) -> {
            if (val == null || val.isNullAt(index) || LogicalTypeRoot.NULL.equals(type.getTypeRoot())) {
                statement.setNull(index, sqlType);
            } else {
                jdbcSerializationConverter.serialize(val, index, statement);
            }
        };
    }

    private int getTimestampPrecision(LogicalType timestampType) {
        if (timestampType instanceof TimestampType) {
            return ((TimestampType) timestampType).getPrecision();
        } else if (timestampType instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) timestampType).getPrecision();
        }
        throw new IllegalArgumentException("unexpected type " + timestampType);
    }

    private int getSqlType(LogicalType type) {
        TypeInformation<?> typeInformation = TypeConversions.fromDataTypeToLegacyInfo(
                TypeConversions.fromLogicalToDataType(type));
        if (EXTENSION_TYPE_MAPPING.containsKey(typeInformation)) {
            return EXTENSION_TYPE_MAPPING.get(typeInformation);
        }
        return JdbcTypeUtil.typeInformationToSqlType(
                TypeConversions.fromDataTypeToLegacyInfo(
                        TypeConversions.fromLogicalToDataType(type)));
    }

    private static <T> T jsonParse(String s, TypeReference<T> type) {
        try {
            return JSON_MAPPER.readValue(s, type);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String toJsonString(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static BinaryStringData[] toArray(List<String> array) {
        return array.stream()
                .map(e -> BinaryStringData.fromString(e))
                .toArray(value -> new BinaryStringData[value]);
    }
}
