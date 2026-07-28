/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import com.google.common.collect.ImmutableMap
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
import de.ii.xtraplatform.features.domain.SortKey
import de.ii.xtraplatform.features.domain.Tuple
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.Function

/**
 * A paged query expression (supportPaging=true) executes the chunked/keyset path while its
 * sub-queries may carry a result-set ('inResultSet') filter. This verifies that the value query
 * composes both: the result-set membership sub-query is kept and the keyset paging window is
 * applied alongside it. A single-shot query (supportPaging=false) keeps the membership but adds no
 * paging.
 */
class ResultSetPagingSpec extends Specification {

    @Shared
    Map<String, SqlQueryMapping> mappings = [:]
    @Shared
    SqlQueryTemplatesDeriver deriver
    @Shared
    SqlMappingDeriver mappingDeriver
    @Shared
    MappingOperationResolver mappingOperationResolver

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        mappingOperationResolver = new MappingOperationResolver()

        ["simple", "value_array"].each { name ->
            def schema = FeatureSchemaFixtures.fromYaml(name)
            def resolvedSchema = schema.accept(mappingOperationResolver, List.of())
            def rules = MappingRuleFixtures.fromYaml(name)
            mappings[name] = mappingDeriver.derive(rules, resolvedSchema).get(0)
        }

        // a filter encoder that can resolve result-set producers, like the one used at runtime
        def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, List.of(), null,
                { type -> Optional.ofNullable(mappings[type]) } as Function)
        deriver = new SqlQueryTemplatesDeriver(filterEncoder, new SqlDialectPgis(), true, false, Optional.empty())
    }

    static InResultSet inResultSet(String producerType, de.ii.xtraplatform.cql.domain.Cql2Expression producerFilter) {
        return new ImmutableInResultSet.Builder()
                .from(InResultSet.of("id", "s1"))
                .producerType(producerType)
                .producerFilter(producerFilter)
                .build()
    }

    List<String> mainValueQuery(int limit, int offset, Optional<Tuple<Object, Object>> minMaxKeys) {
        def schema = FeatureSchemaFixtures.fromYaml("value_array")
        def resolvedSchema = schema.accept(mappingOperationResolver, List.of())
        def rules = MappingRuleFixtures.fromYaml("value_array")
        def templates = mappingDeriver.derive(rules, resolvedSchema).stream().map(deriver.&derive).toList()
        def filter = inResultSet("simple", Eq.of(Property.of("id"), ScalarLiteral.of("foo")))
        return templates.get(0).getValueQueryTemplates()
                .collect { it.generateValueQuery(limit, offset, [] as List<SortKey>, Optional.of(filter), false, minMaxKeys, ImmutableMap.of()) }
    }

    def 'a paged value query keeps the result-set membership and applies the keyset window'() {

        when: 'the main value query is generated with the filter and a keyset window (limit 10, offset 10)'
        String mainQuery = mainValueQuery(10, 10, Optional.of(Tuple.of(10, 19))).get(0)

        then: 'the result-set membership sub-query and the keyset window both appear'
        mainQuery.contains("_rs_0_s1 AS MATERIALIZED")
        mainQuery.contains(">= 10")
        mainQuery.contains("<= 19")
    }

    def 'a single-shot value query keeps the result-set membership but has no paging window'() {

        when: 'the main value query is generated single-shot (no limit, no keyset)'
        String mainQuery = mainValueQuery(0, 0, Optional.<Tuple<Object, Object>> empty()).get(0)

        then: 'the result-set membership is present and there is no LIMIT/OFFSET paging clause'
        mainQuery.contains("_rs_0_s1 AS MATERIALIZED")
        !mainQuery.contains("LIMIT")
        !mainQuery.contains("OFFSET")
    }
}
