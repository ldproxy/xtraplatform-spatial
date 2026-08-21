/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import schemacrawler.crawl.MutableColumn
import schemacrawler.crawl.MutableColumnDataType
import schemacrawler.crawl.MutableTable
import schemacrawler.schema.Column
import schemacrawler.schema.ColumnDataType
import schemacrawler.schema.DataTypeType
import schemacrawler.schema.JavaSqlType
import schemacrawler.schema.JavaSqlTypeGroup
import schemacrawler.schema.Table
import schemacrawler.schemacrawler.SchemaReference
import spock.lang.Specification

import java.sql.JDBCType
import java.sql.Timestamp

/**
 * Detection of columns with a time zone. Only the values of such a column carry an offset in the
 * data source, so the provider option "nativeTimeZone" must not be applied to them; the source
 * schema validator reports the combination as a warning. Drivers disagree about the JDBC type they
 * report for these columns, hence the fallback to the type name.
 */
class SchemaInfoSpec extends Specification {

    def 'temporal columns with and without a time zone'() {

        given: 'a table with the temporal column types of the supported dialects'
        SchemaReference schema = new SchemaReference("catalog", "schema")
        Table table = new MutableTable(schema, "table1")
        addColumn(schema, table, "tstz", "timestamptz", JDBCType.TIMESTAMP)
        addColumn(schema, table, "tstz_jdbc", "timestamptz", JDBCType.TIMESTAMP_WITH_TIMEZONE)
        addColumn(schema, table, "tstz_name", "TIMESTAMP WITH TIME ZONE", JDBCType.TIMESTAMP)
        addColumn(schema, table, "tstz_local", "TIMESTAMP(6) WITH LOCAL TIME ZONE", JDBCType.TIMESTAMP)
        addColumn(schema, table, "ttz", "timetz", JDBCType.TIME)
        addColumn(schema, table, "ts", "timestamp", JDBCType.TIMESTAMP)
        addColumn(schema, table, "ts_name", "timestamp without time zone", JDBCType.TIMESTAMP)
        addColumn(schema, table, "d", "date", JDBCType.DATE)
        SchemaInfo schemaInfo = new SchemaInfo([table])

        expect: 'the columns that carry an offset are detected, independently of the JDBC type'
        schemaInfo.isColumnTemporalWithTimeZone("table1", "tstz")
        schemaInfo.isColumnTemporalWithTimeZone("table1", "tstz_jdbc")
        schemaInfo.isColumnTemporalWithTimeZone("table1", "tstz_name")
        schemaInfo.isColumnTemporalWithTimeZone("table1", "tstz_local")
        schemaInfo.isColumnTemporalWithTimeZone("table1", "ttz")

        and: 'columns without a time zone are not, in particular not "without time zone"'
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "ts")
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "ts_name")
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "d")

        and: 'an unknown column is not reported'
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "unknown")
        !schemaInfo.isColumnTemporalWithTimeZone("unknown", "ts")

        and: 'both are temporal columns'
        schemaInfo.isColumnTemporal("table1", "tstz")
        schemaInfo.isColumnTemporal("table1", "ts")
    }

    def 'a column that is not temporal has no time zone'() {

        given: 'a table with a string and a geometry column'
        SchemaReference schema = new SchemaReference("catalog", "schema")
        Table table = new MutableTable(schema, "table1")
        addColumn(schema, table, "s", "varchar", JDBCType.VARCHAR)
        addColumn(schema, table, "geom", "geometry", JDBCType.OTHER)
        SchemaInfo schemaInfo = new SchemaInfo([table])

        expect:
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "s")
        !schemaInfo.isColumnTemporalWithTimeZone("table1", "geom")
    }

    // the schemacrawler.crawl classes are not public, so every member access goes through Groovy's
    // dynamic dispatch (as in SchemaGeneratorSqlSpec) and the variables are typed as the interfaces
    private static void addColumn(
            SchemaReference schema, Table table, String name, String typeName, JDBCType jdbcType) {
        Column column = new MutableColumn(table, name)
        ColumnDataType columnDataType = new MutableColumnDataType(schema, typeName, DataTypeType.system)
        columnDataType.setJavaSqlType(new JavaSqlType(jdbcType, Timestamp.class, JavaSqlTypeGroup.temporal))
        column.setColumnDataType(columnDataType)
        table.addColumn(column)
    }
}
