/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.jdbc.oceanbase.dialect;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DropOptions;
import org.apache.rocketmq.connect.jdbc.dialect.GenericDatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;


/**
 * OceanBase database dialect
 */
public class OceanBaseDatabaseDialect extends GenericDatabaseDialect {

    private final Logger log = LoggerFactory.getLogger(OceanBaseDatabaseDialect.class);

    /**
     * create OceanBase database dialect
     *
     * @param config
     */
    public OceanBaseDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }


    @Override
    protected String currentTimestampDatabaseQuery() {
        return null;
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        switch (field.schemaType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case INT8:
                return "TINYINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields) {
        List<String> pkFieldNames = this.extractPrimaryKeyFieldNames(fields);
        if (!pkFieldNames.isEmpty()) {
            throw new UnsupportedOperationException("pk is unsupported in OceanBase");
        } else {
            return super.buildCreateTableStatement(table, fields);
        }
    }

    @Override
    protected void writeColumnSpec(ExpressionBuilder builder, SinkRecordField f) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        String sqlType = this.getSqlType(f);
        builder.append(sqlType);
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            this.formatColumnValue(builder, f.schemaType(), f.defaultValue());
        } else if (!this.isColumnOptional(f)) {
            builder.append(" NOT NULL");
        }

    }

    @Override
    public String buildDropTableStatement(TableId table, DropOptions options) {
        ExpressionBuilder builder = this.expressionBuilder();
        builder.append("DROP TABLE ");
        builder.append(table);
        return builder.toString();
    }

    @Override
    public List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields) {
        throw new UnsupportedOperationException("alter is unsupported");
    }

    @Override
    public String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        throw new UnsupportedOperationException("update is unsupported");
    }

    @Override
    public String buildDeleteStatement(TableId table, Collection<ColumnId> keyColumns) {
        throw new UnsupportedOperationException("delete is unsupported");
    }

    @Override
    protected Integer getSqlTypeForSchema(Schema schema) {
        return 0;
    }


    @Override
    protected String sanitizedUrl(String url) {
        // MySQL can also have "username:password@" at the beginning of the host list and
        // in parenthetical properties
        return super.sanitizedUrl(url)
                .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
                .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
    }
}
