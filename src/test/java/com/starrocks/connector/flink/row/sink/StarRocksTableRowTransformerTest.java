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

import com.google.common.base.Strings;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Test;

import mockit.Injectable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.starrocks.connector.flink.StarRocksSinkBaseTest;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.MapDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;

public class StarRocksTableRowTransformerTest extends StarRocksSinkBaseTest {

    private static final LocalDate NESTED_MAP_DATE = LocalDate.of(2021, 1, 2);

    private static final TableSchema NESTED_MAP_SCHEMA = TableSchema.builder()
        .field("m1", DataTypes.MAP(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.STRING())))
        .field("m2", DataTypes.MAP(DataTypes.STRING(), DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())))
        .field("m3", DataTypes.MAP(DataTypes.STRING(), DataTypes.DATE()))
        .field("m4", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
        .field("a1", DataTypes.ARRAY(DataTypes.STRING()))
        .build();

    @SuppressWarnings("unchecked")
    @Test
    public void testTransformer(@Injectable TypeInformation<RowData> rowDataTypeInfo, @Injectable RuntimeContext runtimeCtx) {
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(rowDataTypeInfo);
        rowTransformer.setRuntimeContext(runtimeCtx);
        rowTransformer.setTableSchema(TABLE_SCHEMA);
        GenericRowData rowData = createRowData();
        String result = StarRocksSerializerFactory.createSerializer(OPTIONS, TABLE_SCHEMA.getFieldNames()).serialize(rowTransformer.transform(rowData, OPTIONS.supportUpsertDelete()));

        Map<String, String> loadProsp = OPTIONS.getSinkStreamLoadProperties();
        String format = loadProsp.get("format");
        if (Strings.isNullOrEmpty(format) || "csv".equalsIgnoreCase(format)) {
            assertEquals(TABLE_SCHEMA.getFieldCount(), result.split("\t").length);
        }
        if ("json".equalsIgnoreCase(format)) {
            Map<String, Object> rMap = (Map<String, Object>)JSON.parse(result);
            assertNotNull(rMap);
            assertEquals(TABLE_SCHEMA.getFieldCount(), rMap.size());
            for (String name : TABLE_SCHEMA.getFieldNames()) {
                assertTrue(rMap.containsKey(name));
            }
        }
    }

    @Test
    public void testNestedMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = createNestedMapTransformer();
        assertNestedMapValues(rowTransformer.transform(createNestedMapRowData(), false));
    }

    /**
     * The nested map data of the binary representation should be converted in the same way as
     * the generic representation, and the null value/element should be kept as null.
     */
    @Test
    public void testNestedBinaryMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = createNestedMapTransformer();
        GenericRowData genericRowData = createNestedMapRowData();
        GenericRowData binaryRowData = new GenericRowData(NESTED_MAP_SCHEMA.getFieldCount());
        for (int i = 0; i < NESTED_MAP_SCHEMA.getFieldCount(); i++) {
            LogicalType logicalType = NESTED_MAP_SCHEMA.getFieldDataTypes()[i].getLogicalType();
            if (!LogicalTypeRoot.MAP.equals(logicalType.getTypeRoot())) {
                binaryRowData.setField(i, genericRowData.getField(i));
                continue;
            }
            MapType mapType = (MapType) logicalType;
            binaryRowData.setField(i, new MapDataSerializer(mapType.getKeyType(), mapType.getValueType())
                .toBinaryMap((MapData) genericRowData.getField(i)));
        }
        assertNestedMapValues(rowTransformer.transform(binaryRowData, false));
    }

    private StarRocksTableRowTransformer createNestedMapTransformer() {
        StarRocksTableRowTransformer rowTransformer = new StarRocksTableRowTransformer(null);
        rowTransformer.setRuntimeContext(null);
        rowTransformer.setTableSchema(NESTED_MAP_SCHEMA);
        return rowTransformer;
    }

    private GenericRowData createNestedMapRowData() {
        GenericRowData rowData = new GenericRowData(NESTED_MAP_SCHEMA.getFieldCount());

        Map<Object, Object> m1 = new HashMap<>();
        m1.put(StringData.fromString("a"), new GenericArrayData(new Object[]{StringData.fromString("x"), null}));
        m1.put(StringData.fromString("b"), new GenericArrayData(new Object[]{StringData.fromString("y")}));
        m1.put(StringData.fromString("an"), null);
        rowData.setField(0, new GenericMapData(m1));

        Map<Object, Object> nested = new HashMap<>();
        nested.put(StringData.fromString("nk"), null);
        Map<Object, Object> m2 = new HashMap<>();
        m2.put(StringData.fromString("n"), new GenericMapData(nested));
        m2.put(StringData.fromString("nn"), null);
        rowData.setField(1, new GenericMapData(m2));

        Map<Object, Object> m3 = new HashMap<>();
        m3.put(StringData.fromString("d"), (int) NESTED_MAP_DATE.toEpochDay());
        m3.put(StringData.fromString("dn"), null);
        rowData.setField(2, new GenericMapData(m3));

        Map<Object, Object> m4 = new HashMap<>();
        m4.put(StringData.fromString("s"), null);
        rowData.setField(3, new GenericMapData(m4));

        rowData.setField(4, new GenericArrayData(new Object[]{StringData.fromString("p"), null}));
        return rowData;
    }

    @SuppressWarnings("unchecked")
    private void assertNestedMapValues(Object[] values) {
        // MAP<STRING, ARRAY<STRING>>, the null element of the nested array must be kept as null
        Map<Object, Object> m1 = (Map<Object, Object>) values[0];
        assertEquals(3, m1.size());
        List<Object> a = (List<Object>) m1.get("a");
        assertNotNull(a);
        assertEquals(2, a.size());
        assertEquals(StringData.fromString("x"), a.get(0));
        assertNull(a.get(1));
        List<Object> b = (List<Object>) m1.get("b");
        assertNotNull(b);
        assertEquals(1, b.size());
        assertEquals(StringData.fromString("y"), b.get(0));
        // a null value whose type is an array must be kept as null instead of being traversed
        assertTrue(m1.containsKey("an"));
        assertNull(m1.get("an"));

        // MAP<STRING, MAP<STRING, STRING>>, the null value of the nested map must be kept as null
        Map<Object, Object> m2 = (Map<Object, Object>) values[1];
        assertEquals(2, m2.size());
        Map<Object, Object> n = (Map<Object, Object>) m2.get("n");
        assertNotNull(n);
        assertEquals(1, n.size());
        assertTrue(n.containsKey("nk"));
        assertNull(n.get("nk"));
        // a null value whose type is a map must be kept as null instead of being traversed
        assertTrue(m2.containsKey("nn"));
        assertNull(m2.get("nn"));

        // MAP<STRING, DATE>, the date value should be formatted as a string
        Map<Object, Object> m3 = (Map<Object, Object>) values[2];
        assertEquals(2, m3.size());
        assertEquals(NESTED_MAP_DATE.toString(), m3.get("d"));
        // a null value whose type is a date must be kept as null instead of being formatted
        assertTrue(m3.containsKey("dn"));
        assertNull(m3.get("dn"));

        // MAP<STRING, STRING>, the null value of the map must be kept as null
        Map<Object, Object> m4 = (Map<Object, Object>) values[3];
        assertEquals(1, m4.size());
        assertTrue(m4.containsKey("s"));
        assertNull(m4.get("s"));

        // the null element of the top level array must be kept as null
        List<Object> a1 = (List<Object>) values[4];
        assertEquals(2, a1.size());
        assertEquals(StringData.fromString("p"), a1.get(0));
        assertNull(a1.get(1));
    }

    private GenericRowData createRowData() {
        GenericRowData genericRowData = new GenericRowData(TABLE_SCHEMA.getFieldCount());
        genericRowData.setField(0, (byte)20);
        genericRowData.setField(1, StringData.fromString("xxxssss"));
        genericRowData.setField(2, TimestampData.fromTimestamp(Timestamp.valueOf("2021-02-02 12:22:22.01")));
        genericRowData.setField(3, (int)LocalDate.now().toEpochDay());
        genericRowData.setField(4, DecimalData.fromBigDecimal(BigDecimal.valueOf(1000), 10, 2));
        genericRowData.setField(5, (short)30);
        genericRowData.setField(6, StringData.fromString("ch"));
        return genericRowData;
    }
}
