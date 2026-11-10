/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlFilterExamples
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
 * With `nativeDatetimeComparison`, filters on DATETIME properties compare the column in its own
 * type instead of converting it, so that PostgreSQL can use the statistics and indexes of the
 * column. Without the option, and for properties that need the conversion, the SQL is unchanged.
 */
class FilterEncoderSqlNativeDatetimeSpec extends Specification {

    @Shared
    CqlImpl cql = new CqlImpl()

    @Shared
    SqlQueryMapping mapping

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("id").sortKey("id").build()
        def pathParser = new SqlPathParser(defaults, cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())

        def resolved = resolve(featureType())
        mapping = mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved).get(0)
    }

    FilterEncoderSql encoder(SqlDialect dialect, boolean nativeDatetimeComparison) {
        return new FilterEncoderSql(OgcCrs.CRS84, dialect, null, null, cql, List.of(), null,
                type -> Optional.empty(), Map.of(), nativeDatetimeComparison)
    }

    Cql2Expression filter(String text) {
        return cql.read(text, Cql.Format.TEXT)
    }

    static FeatureSchema featureType() {
        return new ImmutableFeatureSchema.Builder()
                .name("building")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/building")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("id")
                        .role(SchemaBase.Role.ID))
                .putProperties2("updated", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATETIME)
                        .sourcePath("updated"))
                .putProperties2("stamp", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATETIME)
                        .sourcePath("stamp")
                        .format("YYYY-MM-DD HH24:MI:SS"))
                .putProperties2("day", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.DATE)
                        .sourcePath("day"))
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

    def 'without the option, an instant comparison converts the column'() {

        when:
        def actual = encoder(new SqlDialectPgis(), false)
                .encode(filter("T_AFTER(updated, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        actual == "A.updated::timestamp(0) > TIMESTAMP '2026-10-08T00:00:00Z'"
    }

    def 'with the option, an instant comparison uses the column as is'() {

        when:
        def actual = encoder(new SqlDialectPgis(), true)
                .encode(filter("T_AFTER(updated, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        actual == "A.updated > TIMESTAMP '2026-10-08T00:00:00Z'"
    }

    def 'with the option, an interval of instants uses the column as is'() {

        when:
        def actual = encoder(new SqlDialectPgis(), true)
                .encode(filter("T_DURING(updated, INTERVAL('2026-10-08T00:00:00Z','2026-10-23T00:00:00Z'))"), mapping)

        then:
        !actual.contains("::timestamp(0)")
        actual.contains("A.updated")
        actual == encoder(new SqlDialectPgis(), false)
                .encode(filter("T_DURING(updated, INTERVAL('2026-10-08T00:00:00Z','2026-10-23T00:00:00Z'))"), mapping)
                .replace("A.updated::timestamp(0)", "A.updated")
    }

    def 'with the option, a property with a format keeps the conversion of the text value'() {

        when:
        def withOption = encoder(new SqlDialectPgis(), true)
                .encode(filter("T_AFTER(stamp, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)
        def withoutOption = encoder(new SqlDialectPgis(), false)
                .encode(filter("T_AFTER(stamp, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        withOption == withoutOption
        withOption.contains("to_timestamp_tz(A.stamp, 'YYYY-MM-DD HH24:MI:SS')")
    }

    def 'with the option, a DATE property is unchanged'() {

        when:
        def withOption = encoder(new SqlDialectPgis(), true)
                .encode(filter("T_AFTER(day, DATE('2026-10-08'))"), mapping)

        then:
        withOption == encoder(new SqlDialectPgis(), false)
                .encode(filter("T_AFTER(day, DATE('2026-10-08'))"), mapping)
        withOption.contains("A.day::date")
    }

    def 'the option has no effect on other dialects'() {

        when:
        def withOption = encoder(new SqlDialectGpkg(), true)
                .encode(filter("T_AFTER(updated, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)

        then:
        withOption == encoder(new SqlDialectGpkg(), false)
                .encode(filter("T_AFTER(updated, TIMESTAMP('2026-10-08T00:00:00Z'))"), mapping)
        withOption.contains("datetime(A.updated)")
    }

    def 'with the option, the schema-based encoding uses the column as is'() {

        given:
        def schema = QuerySchemaFixtures.SIMPLE_TIMESTAMP

        when:
        def withOption = encoder(new SqlDialectPgis(), true).encode(CqlFilterExamples.EXAMPLE_12, schema)
        def withoutOption = encoder(new SqlDialectPgis(), false).encode(CqlFilterExamples.EXAMPLE_12, schema)

        then:
        withoutOption == "A.built::timestamp(0) < TIMESTAMP '2012-06-05T00:00:00Z'"
        withOption == "A.built < TIMESTAMP '2012-06-05T00:00:00Z'"
    }

    def 'only the comparison variant of the dialect drops the conversion'() {

        expect:
        new SqlDialectPgis().applyToDatetime("A.updated", Optional.empty()) == "A.updated::timestamp(0)"
        new SqlDialectPgis().applyToDatetimeForNativeComparison("A.updated", Optional.empty()) == "A.updated"
        new SqlDialectGpkg().applyToDatetimeForNativeComparison("A.updated", Optional.empty()) == "datetime(A.updated)"
    }
}
