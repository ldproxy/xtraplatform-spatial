/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.ImmutableInResultSet
import de.ii.xtraplatform.cql.domain.InResultSet
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.Function

class FilterEncoderSqlInResultSetSpec extends Specification {

    @Shared
    Map<String, SqlQueryMapping> mappings = [:]
    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        def mappingOperationResolver = new MappingOperationResolver()

        ["simple", "value_array", "simple_filter"].each { name ->
            def schema = FeatureSchemaFixtures.fromYaml(name)
            def resolved = schema.accept(mappingOperationResolver, List.of())
            def rules = MappingRuleFixtures.fromYaml(name)
            mappings[name] = mappingDeriver.derive(rules, resolved).get(0)
        }

        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, List.of(), null,
                { type -> Optional.ofNullable(mappings[type]) } as Function)
    }

    static InResultSet resolved(InResultSet inResultSet, String producerType, de.ii.xtraplatform.cql.domain.Cql2Expression producerFilter) {
        return resolved(inResultSet, producerType, producerFilter, null)
    }

    static InResultSet resolved(InResultSet inResultSet, String producerType, de.ii.xtraplatform.cql.domain.Cql2Expression producerFilter, String values) {
        def builder = new ImmutableInResultSet.Builder()
                .from(inResultSet)
                .producerType(producerType)
        if (producerFilter != null) {
            builder.producerFilter(producerFilter)
        }
        if (values != null) {
            builder.producerValues(values)
        }
        return builder.build()
    }

    def 'plain id set, consumer matches its id queryable'() {
        given: 'a result set over type simple, consumed by a filter on the id queryable'
        def filter = resolved(InResultSet.of("id", "s1"), "simple",
                Eq.of(Property.of("id"), ScalarLiteral.of("foo")))

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id = 'foo')) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'plain id set without a producer filter'() {
        given:
        def filter = resolved(InResultSet.of("id", "s1"), "simple", null)

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'plain id set, consumer matches an array property in a junction table'() {
        given: 'the consumer property is multi-valued, the semantics are like A_OVERLAPS'
        def filter = resolved(InResultSet.of("externalprovidername", "s1"), "simple",
                Eq.of(Property.of("id"), ScalarLiteral.of("foo")))

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA JOIN externalprovider_externalprovidername AB ON (AA.id=AB.externalprovider_fk) WHERE AB.externalprovidername IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id = 'foo')) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'chained result sets nest recursively'() {
        given: 'the producer filter itself consumes another result set'
        def inner = resolved(InResultSet.of("id", "s1"), "simple",
                Eq.of(Property.of("id"), ScalarLiteral.of("foo")))
        def outer = resolved(InResultSet.of("id", "s2"), "value_array", inner)

        when:
        def sql = filterEncoder.encode(outer, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_1_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id = 'foo')), _rs_0_s2 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (SELECT rs_value FROM _rs_1_s1))) SELECT rs_value FROM _rs_0_s2))"
    }

    def 'an unresolved result set reference is rejected'() {
        when:
        filterEncoder.encode(InResultSet.of("id", "s1"), mappings["value_array"])

        then:
        def e = thrown IllegalArgumentException
        e.message.contains("s1")
    }

    def 'an unknown producer type is rejected'() {
        when:
        filterEncoder.encode(resolved(InResultSet.of("id", "s1"), "unknown", null), mappings["value_array"])

        then:
        def e = thrown IllegalArgumentException
        e.message.contains("unknown")
    }

    def 'projected result set over a junction table'() {
        given: 'the set consists of the values referenced by an array property of the selected features'
        def filter = resolved(InResultSet.of("id", "s1"), "value_array",
                Eq.of(Property.of("id"), ScalarLiteral.of("foo")), "externalprovidername")

        when:
        def sql = filterEncoder.encode(filter, mappings["simple"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT B.externalprovidername AS rs_value FROM externalprovider A JOIN externalprovider_externalprovidername B ON (A.id=B.externalprovider_fk) WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id = 'foo')) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'projected result set over a column of the main table'() {
        given:
        def filter = resolved(InResultSet.of("id", "s1"), "simple", null, "id")

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'the filter of the producer main table is applied to the result set'() {
        given:
        def filter = resolved(InResultSet.of("id", "s1"), "simple_filter", null)

        when:
        def sql = filterEncoder.encode(filter, mappings["simple"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN (WITH _rs_0_s1 AS MATERIALIZED (SELECT A.id AS rs_value FROM externalprovider A WHERE A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.type = 1)) SELECT rs_value FROM _rs_0_s1))"
    }

    def 'a materialized result set is inlined as a literal IN list'() {
        given:
        def filter = new ImmutableInResultSet.Builder()
                .from(InResultSet.of("id", "s1"))
                .producerType("simple")
                .materializedValues(["foo", "bar"])
                .build()

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "A.id IN (SELECT AA.id FROM externalprovider AA WHERE AA.id IN ('foo', 'bar'))"
    }

    def 'an empty materialized result set yields a false predicate'() {
        given:
        def filter = new ImmutableInResultSet.Builder()
                .from(InResultSet.of("id", "s1"))
                .producerType("simple")
                .materializedValues([])
                .build()

        when:
        def sql = filterEncoder.encode(filter, mappings["value_array"])

        then:
        sql == "1 = 0"
    }

    def 'an unknown projected property is rejected'() {
        given:
        def filter = resolved(InResultSet.of("id", "s1"), "simple", null, "nosuchproperty")

        when:
        filterEncoder.encode(filter, mappings["value_array"])

        then:
        def e = thrown IllegalArgumentException
        e.message.contains("nosuchproperty")
    }
}
