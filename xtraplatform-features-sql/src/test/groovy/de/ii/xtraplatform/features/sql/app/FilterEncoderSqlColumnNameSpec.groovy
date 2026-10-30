/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.TypesResolver
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

/**
 * A filter on a property must use the column of its source path, which may differ from the
 * property name. For a type with multiple main tables, each table may use a different column.
 */
class FilterEncoderSqlColumnNameSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    SqlMappingDeriver mappingDeriver

    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("id").sortKey("id").build()
        cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of())
        mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
    }

    static ImmutableFeatureSchema.Builder table(String name, String datetimeColumn, String labelColumn) {
        return new ImmutableFeatureSchema.Builder()
                .name(name)
                .sourcePath("/" + name)
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id")
                        .type(SchemaBase.Type.INTEGER)
                        .role(SchemaBase.Role.ID))
                .putProperties2("datetime", new ImmutableFeatureSchema.Builder()
                        .sourcePath(datetimeColumn)
                        .type(SchemaBase.Type.DATETIME)
                        .role(SchemaBase.Role.PRIMARY_INSTANT))
                .putProperties2("label", new ImmutableFeatureSchema.Builder()
                        .sourcePath(labelColumn)
                        .type(SchemaBase.Type.STRING))
    }

    static FeatureSchema singleTable() {
        return table("single", "ts", "name").type(SchemaBase.Type.OBJECT).build()
    }

    static FeatureSchema concat() {
        return new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .concat([
                        table("first", "ts", "name").build(),
                        table("second", "created", "title").build()])
                .build()
    }

    List<SqlQueryMapping> mappings(FeatureSchema type) {
        def resolved = resolve(type)
        return mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved)
    }

    static FeatureSchema resolve(FeatureSchema type) {
        Map<String, FeatureSchema> types = Map.of(type.getName(), type)
        List<TypesResolver> resolvers = List.of(
                new MappingOperationResolver(true),
                new ImplicitMappingResolver(),
                new MappingOperationResolver())

        for (TypesResolver resolver : resolvers) {
            int rounds = 0
            while (resolver.needsResolving(types) && rounds < resolver.maxRounds()) {
                types = resolver.resolve(types)
                rounds++
            }
        }

        return types.get(type.getName())
    }

    String encode(String filter, SqlQueryMapping mapping) {
        filterEncoder.encode(cql.read(filter, Cql.Format.TEXT), mapping)
    }

    def 'A filter uses the column of the property in a single main table'() {
        expect:
        encode(filter, mappings(singleTable()).get(0)) == expected

        where:
        filter                                         | expected
        "datetime > TIMESTAMP('2020-01-01T00:00:00Z')" | "A.ts::timestamp(0) > TIMESTAMP '2020-01-01T00:00:00Z'"
        "label = 'foo'"                                | "A.name = 'foo'"
    }

    def 'A filter uses the column of the property in each of multiple main tables'() {
        expect:
        encode(filter, mappings(concat()).get(table)) == expected

        where:
        filter                                         | table | expected
        "datetime > TIMESTAMP('2020-01-01T00:00:00Z')" | 0     | "A.ts::timestamp(0) > TIMESTAMP '2020-01-01T00:00:00Z'"
        "datetime > TIMESTAMP('2020-01-01T00:00:00Z')" | 1     | "A.created::timestamp(0) > TIMESTAMP '2020-01-01T00:00:00Z'"
        "label = 'foo'"                                | 0     | "A.name = 'foo'"
        "label = 'foo'"                                | 1     | "A.title = 'foo'"
    }
}
