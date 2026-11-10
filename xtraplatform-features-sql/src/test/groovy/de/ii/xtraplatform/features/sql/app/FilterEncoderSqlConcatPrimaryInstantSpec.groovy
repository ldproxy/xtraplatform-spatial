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
 * In a type with multiple main tables, only some of them may have the primary instant. Filters
 * treat it as null in the others.
 */
class FilterEncoderSqlConcatPrimaryInstantSpec extends Specification {

    @Shared
    Cql cql

    @Shared
    List<SqlQueryMapping> mappings

    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("id").sortKey("id").build()
        cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of())
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())

        def resolved = resolve(featureType())
        mappings = mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved)
        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
    }

    static FeatureSchema featureType() {
        def withInstant = new ImmutableFeatureSchema.Builder()
                .name("with_instant")
                .sourcePath("/with_instant")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id")
                        .type(SchemaBase.Type.INTEGER)
                        .role(SchemaBase.Role.ID))
                .putProperties2("datetime", new ImmutableFeatureSchema.Builder()
                        .sourcePath("datetime")
                        .type(SchemaBase.Type.DATETIME)
                        .role(SchemaBase.Role.PRIMARY_INSTANT))
                .build()
        def withoutInstant = new ImmutableFeatureSchema.Builder()
                .name("without_instant")
                .sourcePath("/without_instant")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id")
                        .type(SchemaBase.Type.INTEGER)
                        .role(SchemaBase.Role.ID))
                .build()

        return new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .concat([withInstant, withoutInstant])
                .build()
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

    String encode(String filter, int member) {
        filterEncoder.encode(cql.read(filter, Cql.Format.TEXT), mappings.get(member))
    }

    def 'Each main table gets its own mapping'() {
        expect:
        mappings.size() == 2
    }

    def 'A datetime filter applies to the main table that has the primary instant'() {
        expect:
        encode("datetime > TIMESTAMP('2020-01-01T00:00:00Z')", 0) == "A.datetime::timestamp(0) > TIMESTAMP '2020-01-01T00:00:00Z'"
    }

    def 'A datetime filter matches nothing in the main table without the primary instant'() {
        expect:
        encode(filter, 1) == expected

        where:
        filter                                                    | expected
        "datetime > TIMESTAMP('2020-01-01T00:00:00Z')"            | "1=0"
        "NOT (datetime > TIMESTAMP('2020-01-01T00:00:00Z'))"      | "1=0"
        "datetime IS NULL"                                        | "1=1"
        "datetime > TIMESTAMP('2020-01-01T00:00:00Z') OR id = 1"  | "(1=0 OR A.id = 1)"
    }
}
