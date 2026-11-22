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
 * A DATE or DATETIME value in a JSON document is text. Temporal predicates compare it with date
 * and timestamp literals, so the value must be converted like a text column of the same type.
 */
class FilterEncoderSqlJsonTemporalSpec extends Specification {

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
                .putProperties2("beschluss", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATE)
                        .sourcePath("[JSON]properties/beschluss"))
                .putProperties2("geaendert", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATETIME)
                        .sourcePath("[JSON]properties/geaendert"))
                .putProperties2("name", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("[JSON]properties/name"))
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

    def 'a DATE value in a JSON document is converted to a date'() {

        when:
        def actual = encoder(new SqlDialectPgis()).encode(filter("T_AFTER(beschluss, DATE('2020-01-01'))"), mapping)

        then:
        actual == "(A.properties ->> 'beschluss')::date > DATE '2020-01-01'"
    }

    def 'a DATE value in a JSON document intersects an interval'() {

        when:
        def actual = encoder(new SqlDialectPgis())
                .encode(filter("T_INTERSECTS(beschluss, INTERVAL('2020-01-01','2020-12-31'))"), mapping)

        then:
        actual.contains("(A.properties ->> 'beschluss')::date")
        !actual.contains("(A.properties ->> 'beschluss')::varchar")
        !actual.contains("(A.properties ->> 'beschluss'),")
    }

    def 'a DATETIME value in a JSON document is converted to a timestamp'() {

        when:
        def actual = encoder(new SqlDialectPgis())
                .encode(filter("T_BEFORE(geaendert, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        actual == "(A.properties ->> 'geaendert')::timestamp(0) < TIMESTAMP '2026-10-08T00:00:00Z'"
    }

    def 'a DATE value in a JSON document is converted in GeoPackage'() {

        when:
        def actual = encoder(new SqlDialectGpkg()).encode(filter("T_AFTER(beschluss, DATE('2020-01-01'))"), mapping)

        then:
        actual.startsWith("date(A.properties ->> '\$.beschluss')")
    }

    def 'a STRING value in a JSON document is unchanged'() {

        when:
        def actual = encoder(new SqlDialectPgis()).encode(filter("name = 'x'"), mapping)

        then:
        actual == "(A.properties ->> 'name')::varchar = 'x'"
    }
}
