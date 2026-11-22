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
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.TypesResolver
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialect
import de.ii.xtraplatform.features.sql.domain.SqlDialectGpkg
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

/**
 * A source path with several segments inside a JSON document addresses a nested value. Filters
 * must address the same value, not a key that contains the slashes of the source path.
 */
class FilterEncoderSqlJsonNestedPathSpec extends Specification {

    @Shared
    CqlImpl cql = new CqlImpl()

    @Shared
    SqlQueryMapping mapping

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("pk").sortKey("pk").build()
        def pathParser = new SqlPathParser(defaults, cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())

        def resolved = resolve(featureType())
        mapping = mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved).get(0)
    }

    FilterEncoderSql encoder(SqlDialect dialect) {
        return new FilterEncoderSql(OgcCrs.CRS84, dialect, null, null, cql, null)
    }

    Cql2Expression filter(String text) {
        return cql.read(text, Cql.Format.TEXT)
    }

    static FeatureSchema featureType() {
        return new ImmutableFeatureSchema.Builder()
                .name("plan")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/coretable")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("id")
                        .role(SchemaBase.Role.ID))
                .putProperties2("autor", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("[JSON]properties/meta/autor"))
                .putProperties2("geaendert", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATETIME)
                        .sourcePath("[JSON]properties/meta/geaendert"))
                .putProperties2("stufe", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT)
                        .sourcePath("[JSON]properties/verfahren")
                        .putProperties2("name", new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.STRING)
                                .sourcePath("stufe/name")))
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

    def 'a value in a nested object of a JSON document is addressed by its path'() {

        when:
        def actual = encoder(new SqlDialectPgis()).encode(filter("autor = 'x'"), mapping)

        then:
        actual == "(A.properties #>> '{meta,autor}')::varchar = 'x'"
    }

    def 'a nested DATETIME value in a JSON document is converted to a timestamp'() {

        when:
        def actual = encoder(new SqlDialectPgis())
                .encode(filter("T_BEFORE(geaendert, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        actual == "(A.properties #>> '{meta,geaendert}')::timestamp(0) < TIMESTAMP '2026-10-08T00:00:00Z'"
    }

    def 'a nested path below an object property is addressed by its path'() {

        when:
        def actual = encoder(new SqlDialectPgis()).encode(filter("stufe.name = 'x'"), mapping)

        then:
        actual == "(A.properties #>> '{verfahren,stufe,name}')::varchar = 'x'"
    }

    def 'a value in a nested object of a JSON document is addressed by its path in GeoPackage'() {

        when:
        def actual = encoder(new SqlDialectGpkg()).encode(filter("autor = 'x'"), mapping)

        then:
        actual == "cast((A.properties ->> '\$.meta.autor') as text) = 'x'"
    }
}
