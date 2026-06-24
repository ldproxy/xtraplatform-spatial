/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Cql2Expression
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.ImmutableInResultSet
import de.ii.xtraplatform.cql.domain.InResultSet
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.ImmutableMultiFeatureQuery
import de.ii.xtraplatform.features.domain.ImmutableSubQuery
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.domain.MultiFeatureQuery
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlClient
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.SqlRow
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.function.Function
import java.util.function.Supplier

class ResultSetMaterializerSpec extends Specification {

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

        ["simple", "value_array"].each { name ->
            def schema = FeatureSchemaFixtures.fromYaml(name)
            def resolved = schema.accept(mappingOperationResolver, List.of())
            def rules = MappingRuleFixtures.fromYaml(name)
            mappings[name] = mappingDeriver.derive(rules, resolved).get(0)
        }

        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, List.of(), null,
                { type -> Optional.ofNullable(mappings[type]) } as Function)
    }

    static InResultSet resolved(String setName, String producerType, Cql2Expression producerFilter) {
        return new ImmutableInResultSet.Builder()
                .from(InResultSet.of("id", setName))
                .producerType(producerType)
                .producerFilter(producerFilter)
                .build()
    }

    SqlRow row(Object value) {
        return Stub(SqlRow) {
            getValues() >> [value]
        }
    }

    static MultiFeatureQuery query(Cql2Expression filter) {
        return ImmutableMultiFeatureQuery.builder()
                .addQueries(
                        ImmutableSubQuery.builder()
                                .collectionId("c")
                                .type("value_array")
                                .addFilters(filter)
                                .build())
                .build()
    }

    def 'a result set is materialized once and its values are attached to the consumer'() {
        given:
        def filter = resolved("s1", "simple", Eq.of(Property.of("id"), ScalarLiteral.of("foo")))
        def sqlClient = Mock(SqlClient)
        def materializer = new ResultSetMaterializer({ -> sqlClient } as Supplier, filterEncoder, 100000)

        when:
        def result = materializer.materialize(query(filter))

        then:
        1 * sqlClient.run(_, _) >> CompletableFuture.completedFuture([row("x"), row("y")])
        def node = (InResultSet) result.getQueries().get(0).getFilters().get(0)
        node.getMaterializedValues().get() == ["x", "y"]
    }

    def 'a result set exceeding the cap is left unmaterialized'() {
        given:
        def filter = resolved("s1", "simple", Eq.of(Property.of("id"), ScalarLiteral.of("foo")))
        def sqlClient = Mock(SqlClient)
        def materializer = new ResultSetMaterializer({ -> sqlClient } as Supplier, filterEncoder, 1)

        when:
        def result = materializer.materialize(query(filter))

        then:
        1 * sqlClient.run(_, _) >> CompletableFuture.completedFuture([row("x"), row("y")])
        def node = (InResultSet) result.getQueries().get(0).getFilters().get(0)
        node.getMaterializedValues().isEmpty()
    }

    def 'a query without result sets is returned unchanged'() {
        given:
        def filter = Eq.of(Property.of("id"), ScalarLiteral.of("foo"))
        def sqlClient = Mock(SqlClient)
        def materializer = new ResultSetMaterializer({ -> sqlClient } as Supplier, filterEncoder, 100000)

        when:
        def result = materializer.materialize(query(filter))

        then:
        0 * sqlClient.run(_, _)
        result == query(filter)
    }
}
