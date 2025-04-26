/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.*
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.*
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.*
import spock.lang.Shared
import spock.lang.Specification

class SqlQueryTemplatesDeriver2Spec extends Specification {

    @Shared
    FilterEncoderSql filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, new CqlImpl(), null)
    @Shared
    SqlQueryTemplatesDeriver2 td = new SqlQueryTemplatesDeriver2(filterEncoder, new SqlDialectPgis(), true, false, Optional.empty())
    @Shared
    SqlQueryTemplatesDeriver2 tdNoNm = new SqlQueryTemplatesDeriver2(filterEncoder, new SqlDialectPgis(), false, false, Optional.empty())
    @Shared
    SqlMappingDeriver mappingDeriver
    @Shared
    MappingOperationResolver mappingOperationResolver

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        //def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson()))

        mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        mappingOperationResolver = new MappingOperationResolver()

    }

    static Optional<Cql2Expression> noFilter = Optional.empty()

    def 'meta query templates: #casename'() {

        when:

        def schema = FeatureSchemaFixtures.fromYaml(rules)
        def resolved = schema.accept(mappingOperationResolver, List.of())
        def mappingRules = MappingRuleFixtures.fromYaml(rules)
        SqlQueryMapping mapping = mappingDeriver.derive(mappingRules, resolved)
        SqlQueryTemplates templates = deriver.derive(mapping)
        String actual = meta(templates, sortBy, userFilter)

        then:

        actual == expected

        where:

        casename                      | deriver | sortBy                                                                            | userFilter | rules           || expected
        "basic"                       | td      | []                                                                                | noFilter   | "simple"        || SqlQueryTemplatesFixtures.META
        "basic without numberMatched" | tdNoNm  | []                                                                                | noFilter   | "simple"        || SqlQueryTemplatesFixtures.META_WITHOUT_NUMBER_MATCHED
        "sortBy"                      | td      | [SortKey.of("created")]                                                           | noFilter   | "simple"        || SqlQueryTemplatesFixtures.META_SORT_BY
        "sortBy descending"           | td      | [SortKey.of("created", SortKey.Direction.DESCENDING)]                             | noFilter   | "simple"        || SqlQueryTemplatesFixtures.META_SORT_BY_DESC
        "sortBy mixed"                | td      | [SortKey.of("created", SortKey.Direction.DESCENDING), SortKey.of("lastModified")] | noFilter   | "simple"        || SqlQueryTemplatesFixtures.META_SORT_BY_MIXED
        "filter"                      | td      | []                                                                                | noFilter   | "simple_filter" || SqlQueryTemplatesFixtures.META_FILTER
    }

    def 'value query templates: #casename'() {

        when:

        def schema = FeatureSchemaFixtures.fromYaml(rules)
        def resolved = schema.accept(mappingOperationResolver, List.of())
        def mappingRules = MappingRuleFixtures.fromYaml(rules)
        SqlQueryMapping mapping = mappingDeriver.derive(mappingRules, resolved)
        SqlQueryTemplates templates = deriver.derive(mapping)
        List<String> actual = values(templates, limit, offset, sortBy, filter)
        List<String> expected = SqlQueryFixtures.fromYaml(queries)

        then:

        actual == expected

        where:

        //TODO: column ops
        casename                                      | deriver | limit | offset | sortBy                  | filter                                                                                                                      | rules                                   || queries
        "simple"                                      | td      | 0     | 0      | []                      | null                                                                                                                        | "simple"                                || "simple"
        "simple filter"                               | td      | 0     | 0      | []                      | null                                                                                                                        | "simple_filter"                         || "simple_filter"
        "value array"                                 | td      | 0     | 0      | []                      | null                                                                                                                        | "value_array"                           || "value_array"
        "object array"                                | td      | 0     | 0      | []                      | null                                                                                                                        | "object_array"                          || "object_array"
        "merge"                                       | td      | 0     | 0      | []                      | null                                                                                                                        | "merge"                                 || "merge"
        "self joins"                                  | td      | 0     | 0      | []                      | null                                                                                                                        | "self_joins"                            || "self_joins"
        "self joins with filters"                     | td      | 0     | 0      | []                      | null                                                                                                                        | "self_joins_filter"                     || "self_joins_filter"
        "self joins with nested duplicate join"       | td      | 0     | 0      | []                      | null                                                                                                                        | "self_joins_with_nested_duplicate_join" || "self_joins_with_nested_duplicate_join"
        "object without sourcePath"                   | td      | 0     | 0      | []                      | null                                                                                                                        | "object_without_sourcePath"             || "object_without_sourcePath"
        "paging"                                      | td      | 10    | 10     | []                      | null                                                                                                                        | "object_array"                          || "object_array_paging"
        "sortBy"                                      | td      | 0     | 0      | [SortKey.of("created")] | null                                                                                                                        | "object_array"                          || "object_array_sortby"
        "sortBy + filter"                             | td      | 0     | 0      | [SortKey.of("created")] | Eq.of(Property.of("task.title"), ScalarLiteral.of("foo"))                                                                   | "object_array"                          || "object_array_sortby_filter"
        "sortBy + paging"                             | td      | 10    | 10     | [SortKey.of("created")] | null                                                                                                                        | "object_array"                          || "object_array_sortby_paging"
        "sortBy + paging + filter"                    | td      | 10    | 10     | [SortKey.of("created")] | And.of(Eq.of(Property.of("task.title"), ScalarLiteral.of("foo")), Eq.of(Property.of("task.href"), ScalarLiteral.of("bar"))) | "object_array"                          || "object_array_sortby_paging_filter"
        "concat values"                               | td      | 0     | 0      | []                      | null                                                                                                                        | "concat_values"                         || "property_with_multiple_sourcePaths"
        "nested joins"                                | td      | 0     | 0      | []                      | null                                                                                                                        | "nested_joins"                          || "nested_joins"
        "connector"                                   | td      | 0     | 0      | []                      | null                                                                                                                        | "simple_connector"                      || "connector"
        //"concat object arrays"                        | td      | 0     | 0      | []                      | null                                                                                                                        | "pfs_plan-hatObjekt"                    || "pfs_plan-hatObjekt"
        "concat root objects"                         | td      | 0     | 0      | []                      | null                                                                                                                        | "concat_root_objects"                   || "concat_root_objects"
        "long value path overlap"                     | td      | 0     | 0      | []                      | null                                                                                                                        | "long_value_path_overlap"               || "long_value_path_overlap"
        "self join with nested duplicate and filters" | td      | 0     | 0      | []                      | null                                                                                                                        | "okstra_abschnitt"                      || "okstra_abschnitt"
        "embedded object with concat and backlink"    | td      | 0     | 0      | []                      | null                                                                                                                        | "pfs_plan-hatObjekt-embedded"           || "pfs_plan-hatObjekt-embedded"
        //TODO: constants in concat value array
        //"root concat with value concat with constant" | td      | 0     | 0      | []                      | null                                                                                                                        | "landcoverunit"                         || "landcoverunit"
        "strassen_unfaelle2"                          | td      | 0     | 0      | []                      | null                                                                                                                        | "strassen_unfaelle2"                    || "strassen_unfaelle2"
    }


    static String meta(SqlQueryTemplates templates, List<SortKey> sortBy, Optional<Cql2Expression> userFilter) {
        return templates.getMetaQueryTemplate().generateMetaQuery(10, 10, 0, sortBy, userFilter, ImmutableMap.of(), false, true)
    }

    static List<String> values(SqlQueryTemplates templates, int limit, int offset, List<SortKey> sortBy, Cql2Expression filter) {
        return templates.getValueQueryTemplates().collect { it.generateValueQuery(limit, offset, sortBy, Optional.ofNullable(filter), limit == 0 ? Optional.<Tuple<Object, Object>> empty() : Optional.of(Tuple.of(offset, offset + limit - 1)), ImmutableMap.of()) }
    }
}
