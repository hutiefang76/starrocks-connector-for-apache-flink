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

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;

import java.util.Map;
import java.util.Objects;

public class StarRocksSerializerFactory {

    private static final String COLUMN_SEPARATOR = "column_separator";
    private static final String ROW_DELIMITER = "row_delimiter";
    private static final String ENCLOSE = "enclose";
    private static final String ESCAPE = "escape";

    private StarRocksSerializerFactory() {}

    public static StarRocksISerializer createSerializer(StarRocksSinkOptions sinkOptions, String[] fieldNames) {
        if (StarRocksSinkOptions.StreamLoadFormat.CSV.equals(sinkOptions.getStreamLoadFormat())) {
            Map<String, String> streamLoadProperties = sinkOptions.getSinkStreamLoadProperties();
            rejectTableDialectOverride(sinkOptions, streamLoadProperties);
            return new StarRocksCsvSerializer(
                    streamLoadProperties.get(COLUMN_SEPARATOR),
                    streamLoadProperties.get(ROW_DELIMITER),
                    streamLoadProperties.get(ENCLOSE),
                    streamLoadProperties.get(ESCAPE));
        }
        if (StarRocksSinkOptions.StreamLoadFormat.JSON.equals(sinkOptions.getStreamLoadFormat())) {
            if (sinkOptions.supportUpsertDelete()) {
                String[] tmp = new String[fieldNames.length + 1];
                System.arraycopy(fieldNames, 0, tmp, 0, fieldNames.length);
                tmp[fieldNames.length] = StarRocksSinkOP.COLUMN_KEY;
                fieldNames = tmp;
            }
            return new StarRocksJsonSerializer(fieldNames);
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }

    /**
     * The csv serializer writes every row with the sink level dialect, while a V2 sink replaces those
     * headers with the ones registered for its own table, so an override that changes the dialect
     * makes the server parse a different csv than the one that was written: quote bytes are loaded
     * literally and an embedded separator splits a row. The serializer cannot follow the override
     * either, because a V1 sink sends the sink level headers only and resolving an override goes
     * through the frontend. Reject the combination instead of corrupting the load.
     */
    private static void rejectTableDialectOverride(StarRocksSinkOptions sinkOptions,
                                                   Map<String, String> streamLoadProperties) {
        for (Map.Entry<String, String> override : sinkOptions.getTableCsvDialectOverrides().entrySet()) {
            String header = override.getKey();
            // Both sides are resolved the way StarRocksCsvSerializer resolves them, so an equivalent
            // setting, such as "\t" for a separator that was left at its default, still passes.
            if (!Objects.equals(normalizeDialect(header, override.getValue()),
                    normalizeDialect(header, streamLoadProperties.get(header)))) {
                throw new IllegalArgumentException(String.format(
                        "The properties registered for table %s.%s set `%s` to %s, but the csv serializer "
                                + "writes every row with the sink level `%s`, %s. A per table csv dialect is not "
                                + "supported, the bytes and the load headers would disagree. Set %s%s on the sink "
                                + "instead.",
                        sinkOptions.getDatabaseName(), sinkOptions.getTableName(),
                        header, display(override.getValue()),
                        header, display(streamLoadProperties.get(header)),
                        StarRocksSinkOptions.SINK_PROPERTIES_PREFIX, header));
            }
        }
    }

    private static String normalizeDialect(String header, String value) {
        if (COLUMN_SEPARATOR.equals(header)) {
            return StarRocksDelimiterParser.parse(value, "\t");
        }
        if (ROW_DELIMITER.equals(header)) {
            return StarRocksDelimiterParser.parse(value, "\n");
        }
        return StarRocksCsvSerializer.singleByteOption(value, header);
    }

    private static String display(String value) {
        return value == null || value.isEmpty() ? "unset" : "`" + value + "`";
    }
}
