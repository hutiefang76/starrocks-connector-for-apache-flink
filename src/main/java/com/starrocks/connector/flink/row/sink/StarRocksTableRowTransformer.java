/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.row.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.connector.flink.table.StarRocksDataType;
import com.starrocks.connector.flink.tools.JsonWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StarRocksTableRowTransformer implements StarRocksIRowTransformer<RowData> {

    private static final long serialVersionUID = 1L;

    private TypeInformation<RowData> rowDataTypeInfo;
    private Function<RowData, RowData> valueTransform;
    private String[] columnNames;
    private DataType[] columnDataTypes;
    private Map<String, StarRocksDataType> columns;
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    private transient JsonWrapper jsonWrapper;

    public StarRocksTableRowTransformer(TypeInformation<RowData> rowDataTypeInfo) {
        this.rowDataTypeInfo = rowDataTypeInfo;
    }

    @Override
    public void setStarRocksColumns(Map<String, StarRocksDataType> columns) {
        this.columns = columns;
    }

    @Override
    public void setTableSchema(TableSchema ts) {
        this.columnNames = ts.getFieldNames();
        this.columnDataTypes = ts.getFieldDataTypes();
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeCtx) {
        // No need to copy the value even if object reuse is enabled,
        // because the raw RowData value will not be buffered
        this.valueTransform = Function.identity();
    }

    @Override
    public void setFastJsonWrapper(JsonWrapper jsonWrapper) {
        this.jsonWrapper = jsonWrapper;
    }

    @Override
    public Object[] transform(RowData record, boolean supportUpsertDelete) {
        RowData transformRecord = valueTransform.apply(record);
        Object[] values = new Object[columnDataTypes.length + (supportUpsertDelete ? 1 : 0)];
        int idx = 0;
        for (DataType dataType : columnDataTypes) {
            values[idx] = typeConvertion(dataType.getLogicalType(), transformRecord, idx);
            idx++;
        }
        if (supportUpsertDelete) {
            // set `__op` column
            values[idx] = StarRocksSinkOP.parse(record.getRowKind()).ordinal();
        }
        return values;
    }

    private Object typeConvertion(LogicalType type, RowData record, int pos) {
        return typeConvertion(type, record, pos, true);
    }

    /**
     * Convert the value at the given position of a row. Only a top level position may look up the
     * metadata (json/string) of the starrocks columns, because the position of a nested field is
     * relative to its own nested row.
     */
    private Object typeConvertion(LogicalType type, RowData record, int pos, boolean topLevel) {
        if (record.isNullAt(pos)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case BOOLEAN: 
                return record.getBoolean(pos) ? 1L : 0L;
            case TINYINT:
                return record.getByte(pos);
            case SMALLINT:
                return record.getShort(pos);
            case INTEGER:
                return record.getInt(pos);
            case BIGINT:
                return record.getLong(pos);
            case FLOAT:
                return record.getFloat(pos);
            case DOUBLE:
                return record.getDouble(pos);
            case CHAR:
            case VARCHAR:
                String sValue = record.getString(pos).toString();
                if (!topLevel || columns == null) {
                    return sValue;
                }
                StarRocksDataType starRocksDataType =
                        columns.getOrDefault(columnNames[pos], StarRocksDataType.UNKNOWN);
                if ((starRocksDataType == StarRocksDataType.JSON ||
                        starRocksDataType == StarRocksDataType.UNKNOWN)
                    && !sValue.isEmpty() && (sValue.charAt(0) == '{' || sValue.charAt(0) == '[')) {
                    // The json string need to be converted to a json object, and to the json string
                    // again via JSON.toJSONString in StarRocksJsonSerializer#serialize. Otherwise,
                    // the final json string in stream load will not be correct. For example, the received
                    // string is "{"a": 1, "b": 2}", and if input it to JSON.toJSONString directly, the
                    // result will be "{\"a\": 1, \"b\": 2}" which will not be recognized as a json in
                    // StarRocks
                    return jsonWrapper.parse(sValue);
                }
                return sValue;
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(record.getInt(pos))));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision =((TimestampType) type).getPrecision();
                return record.getTimestamp(pos, timestampPrecision).toLocalDateTime().toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                int localZonedTimestampPrecision =((LocalZonedTimestampType) type).getPrecision();
                return record.getTimestamp(pos, localZonedTimestampPrecision).toLocalDateTime().toString();
            case DECIMAL: // for both largeint and decimal
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                return record.getDecimal(pos, decimalPrecision, decimalScale).toBigDecimal();
            case BINARY:
                final byte[] bts = record.getBinary(pos);
                long value = 0;
                for (int i = 0; i < bts.length; i++) {
                    value += (bts[bts.length - i - 1] & 0xffL) << (8 * i);
                }
                return value;
            case ARRAY:
                return convertNestedArray(record.getArray(pos), type);
            case MAP:
                return convertNestedMap(record.getMap(pos), type);
            case ROW:
                RowType rType = (RowType)type;
                Map<String, Object> m = convertNestedRow(record.getRow(pos, rType.getFieldCount()), rType);
                if (!topLevel || columns == null) {
                    return m;
                }
                StarRocksDataType rStarRocksDataType =
                        columns.getOrDefault(columnNames[pos], StarRocksDataType.UNKNOWN);
                if (rStarRocksDataType == StarRocksDataType.STRING) {
                    return jsonWrapper.toJSONString(m);
                }
                return m;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private List<Object> convertNestedArray(ArrayData arrData, LogicalType type) {
        if (arrData instanceof GenericArrayData) {
            return Lists.newArrayList(((GenericArrayData)arrData).toObjectArray());
        }
        if (arrData instanceof BinaryArrayData) {
            LogicalType lt = ((ArrayType)type).getElementType();
            List<Object> data = Lists.newArrayList(((BinaryArrayData)arrData).toObjectArray(lt));
            if (LogicalTypeRoot.ROW.equals(lt.getTypeRoot())) {
                RowType rType = (RowType)lt;
                // parse nested row data
                return data.parallelStream().map(row -> {
                    if (null == row) {
                        // the null element of the nested array must be kept as null
                        return null;
                    }
                    return jsonWrapper.toJSONString(convertNestedRow((RowData)row, rType));
                }).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.MAP.equals(lt.getTypeRoot())) {
                // traversal of the nested map
                return data.parallelStream().map(m -> null == m ? null : convertNestedMap((MapData)m, lt)).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.DATE.equals(lt.getTypeRoot())) {
                return data.parallelStream().map(date -> null == date ? null : dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay((Integer)date)))).collect(Collectors.toList());
            }
            if (LogicalTypeRoot.ARRAY.equals(lt.getTypeRoot())) {
                // traversal of the nested array
                return data.parallelStream().map(arr -> null == arr ? null : convertNestedArray((ArrayData)arr, lt)).collect(Collectors.toList());
            }
            return data;
        }
        throw new UnsupportedOperationException(String.format("Unsupported array data: %s", arrData.getClass()));
    }

    private Map<Object, Object> convertNestedMap(MapData mapData, LogicalType type) {
        LogicalType keyType = ((MapType)type).getKeyType();
        LogicalType valueType = ((MapType)type).getValueType();
        ArrayData keyArray = mapData.keyArray();
        ArrayData valueArray = mapData.valueArray();
        Map<Object, Object> result = Maps.newHashMap();
        for (int i = 0; i < mapData.size(); i++) {
            // The key must be a string: fastjson writes a key which is not a string without
            // quotes, and the produced json object is rejected by StarRocks. A null value is kept
            // as null instead of being traversed as a nested map/array.
            result.put(convertNestedMapKey(keyArray, i, keyType), convertNestedElement(valueArray, i, valueType));
        }
        return result;
    }

    /**
     * Convert the key of a nested map to the string which is used as the field name of the json
     * object, formatted in the same way as the map values so that the keys do not depend on the
     * serializers of fastjson.
     */
    private Object convertNestedMapKey(ArrayData keyArray, int pos, LogicalType keyType) {
        if (keyArray.isNullAt(pos)) {
            return null;
        }
        switch (keyType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return keyArray.getString(pos).toString();
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(keyArray.getInt(pos))));
            case DECIMAL:
                final DecimalType keyDecimalType = (DecimalType) keyType;
                return keyArray.getDecimal(pos, keyDecimalType.getPrecision(), keyDecimalType.getScale())
                        .toBigDecimal().toPlainString();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return keyArray.getTimestamp(pos, ((TimestampType) keyType).getPrecision())
                        .toLocalDateTime().toString();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return keyArray.getTimestamp(pos, ((LocalZonedTimestampType) keyType).getPrecision())
                        .toLocalDateTime().toString();
            default:
                return String.valueOf(convertNestedElement(keyArray, pos, keyType));
        }
    }

    /**
     * Convert a single element of a nested map or array to a plain java object according to its
     * logical type, because the internal types like StringData can not be serialized correctly
     * as the values of a map are passed to the json serializer as they are. A null element is
     * kept as null.
     */
    private Object convertNestedElement(ArrayData arrayData, int pos, LogicalType type) {
        if (arrayData.isNullAt(pos)) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return arrayData.getString(pos).toString();
            case DATE:
                return dateFormatter.format(Date.valueOf(LocalDate.ofEpochDay(arrayData.getInt(pos))));
            case MAP:
                return convertNestedMap(arrayData.getMap(pos), type);
            case ARRAY:
                return convertNestedArray(arrayData.getArray(pos), type);
            case ROW:
                RowType rType = (RowType)type;
                return convertNestedRow(arrayData.getRow(pos, rType.getFieldCount()), rType);
            default:
                return ArrayData.createElementGetter(type).getElementOrNull(arrayData, pos);
        }
    }

    /**
     * Convert the fields of a nested row to a plain map. The position of a nested field is
     * relative to its own nested row, so the conversion must not look up the metadata of the top
     * level columns: it would apply the metadata of another column, or be out of range.
     */
    private Map<String, Object> convertNestedRow(RowData row, RowType rowType) {
        Map<String, Object> m = new HashMap<>();
        for (RowType.RowField field : rowType.getFields()) {
            m.put(field.getName(), typeConvertion(field.getType(), row, rowType.getFieldIndex(field.getName()), false));
        }
        return m;
    }
    
}
