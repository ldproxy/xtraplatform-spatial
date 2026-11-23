/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import de.ii.xtraplatform.entities.domain.ValidationResult
import de.ii.xtraplatform.entities.domain.ValidationResult.MODE
import de.ii.xtraplatform.features.domain.SchemaBase.Type
import de.ii.xtraplatform.features.sql.domain.ImmutableSchemaSql
import de.ii.xtraplatform.features.sql.domain.SchemaSql
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

/**
 * Validation of the source schema of a type against the tables and columns of the database. A
 * value with a SQL expression as its source path ("[EXPRESSION]{sql=...}") is computed by the
 * database and has no column of its own; it is not checked against the columns of the table.
 */
class SourceSchemaValidatorSqlSpec extends Specification {

    SourceSchemaValidatorSql validator = new SourceSchemaValidatorSql([], Optional.empty(), { null })
    SchemaInfo schemaInfo

    def setup() {
        SchemaReference schema = new SchemaReference("catalog", "schema")
        Table table = new MutableTable(schema, "building")
        addColumn(schema, table, "id")
        addColumn(schema, table, "name")
        schemaInfo = new SchemaInfo([table])
    }

    def "a value with a SQL expression is not checked as a column"() {
        when:
        ValidationResult result =
                validator.validate("building", building(value("SQL__1", true)), MODE.LAX, schemaInfo)

        then:
        result.getErrors().isEmpty()
    }

    def "a value without a column is an error"() {
        when:
        ValidationResult result =
                validator.validate("building", building(value("SQL__1", false)), MODE.LAX, schemaInfo)

        then:
        result.getErrors().size() == 1
        result.getErrors()[0].contains("column 'SQL__1' in table 'building' does not exist")
    }

    def "a value with a column is valid"() {
        when:
        ValidationResult result =
                validator.validate("building", building(value("name", false)), MODE.LAX, schemaInfo)

        then:
        result.getErrors().isEmpty()
    }

    private static SchemaSql building(SchemaSql property) {
        return new ImmutableSchemaSql.Builder()
                .name("building")
                .sourcePath("building")
                .type(Type.OBJECT)
                .sortKey("id")
                .addProperties(property)
                .build()
    }

    private static SchemaSql value(String column, boolean isExpression) {
        return new ImmutableSchemaSql.Builder()
                .name(column)
                .sourcePath(column)
                .type(Type.STRING)
                .parentPath(["building"])
                .isExpression(isExpression)
                .build()
    }

    private static void addColumn(SchemaReference schema, Table table, String name) {
        Column column = new MutableColumn(table, name)
        ColumnDataType columnDataType = new MutableColumnDataType(schema, "text", DataTypeType.system)
        columnDataType.setJavaSqlType(new JavaSqlType(JDBCType.VARCHAR, String.class, JavaSqlTypeGroup.character))
        column.setColumnDataType(columnDataType)
        table.addColumn(column)
    }
}
