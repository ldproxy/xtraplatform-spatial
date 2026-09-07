/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase
import spock.lang.Specification
import spock.lang.Unroll

/**
 * Whether the database assigns the id of a new feature on insert. The four variants of the
 * documentation of the SQL feature provider:
 *
 * <ul>
 *   <li>the column of the property with the role ID is the primary key: the id is generated,
 *   <li>the same column with the flag {@code {generated=false}}: the value from the request body
 *       is inserted,
 *   <li>another column: the value from the request body is inserted, unless the source path of
 *       the table generates it with an {@code inserts} flag (not covered by this predicate).
 * </ul>
 *
 * The decision drives the id that a create reports: the id that the insert returns where the
 * database assigns it, the value in the written row where the client does.
 */
class GeneratedFeatureIdSpec extends Specification {

    @Unroll
    def 'the id is generated on insert: #expected — #variant'() {
        expect:
        mapping(primaryKey, idColumn, doNotGenerate).hasGeneratedId() == expected

        where:
        variant                                  | primaryKey | idColumn | doNotGenerate || expected
        'primary key, auto-generated'            | 'id'       | 'id'     | false         || true
        'primary key with {generated=false}'     | 'id'       | 'id'     | true          || false
        'another column (e.g. an ALKIS objid)'   | 'id'       | 'objid'  | false         || false
        'another column, {generated=false}'      | 'id'       | 'objid'  | true          || false
    }

    def 'a type without a property with the role ID reports a generated id'() {
        given: 'a mapping whose only column is a geometry'
        SqlQueryColumn geometry = new ImmutableSqlQueryColumn.Builder()
                .name('geom')
                .pathSegment('geom')
                .type(SchemaBase.Type.GEOMETRY)
                .role(SchemaBase.Role.PRIMARY_GEOMETRY)
                .schemaIndex(0)
                .build()
        SqlQuerySchema table = new ImmutableSqlQuerySchema.Builder()
                .name('buildings')
                .pathSegment('buildings')
                .addColumns(geometry)
                .build()
        SqlQueryMapping mapping = new ImmutableSqlQueryMapping.Builder()
                .addTables(table)
                .mainSchema(new ImmutableFeatureSchema.Builder()
                        .name('buildings')
                        .type(SchemaBase.Type.OBJECT)
                        .sourcePath('/buildings')
                        .putProperties2('geom', new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.GEOMETRY)
                                .sourcePath('geom')
                                .role(SchemaBase.Role.PRIMARY_GEOMETRY))
                        .build())
                .putValueTables('geom', table)
                .putValueColumns('geom', geometry)
                .build()

        expect: 'nothing in the request body can be the id of the new feature'
        mapping.hasGeneratedId()
    }

    static SqlQueryMapping mapping(String primaryKey, String idColumn, boolean doNotGenerate) {
        SqlQueryColumn column = new ImmutableSqlQueryColumn.Builder()
                .name(idColumn)
                .pathSegment(idColumn)
                .type(SchemaBase.Type.STRING)
                .role(SchemaBase.Role.ID)
                .operations(doNotGenerate
                        ? Map.of(SqlQueryColumn.Operation.DO_NOT_GENERATE, [] as String[])
                        : Map.of())
                .schemaIndex(0)
                .build()
        SqlQuerySchema table = new ImmutableSqlQuerySchema.Builder()
                .name('buildings')
                .pathSegment('buildings')
                .primaryKey(primaryKey)
                .addColumns(column)
                .build()

        return new ImmutableSqlQueryMapping.Builder()
                .addTables(table)
                .mainSchema(new ImmutableFeatureSchema.Builder()
                        .name('buildings')
                        .type(SchemaBase.Type.OBJECT)
                        .sourcePath('/buildings')
                        .putProperties2('id', new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.STRING)
                                .sourcePath(idColumn)
                                .role(SchemaBase.Role.ID))
                        .build())
                .putValueTables('id', table)
                .putValueColumns('id', column)
                .build()
    }
}
