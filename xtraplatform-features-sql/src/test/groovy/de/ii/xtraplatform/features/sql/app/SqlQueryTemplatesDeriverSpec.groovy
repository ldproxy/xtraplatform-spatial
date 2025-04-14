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
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.SortKey
import de.ii.xtraplatform.features.domain.Tuple
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ConstantsResolver
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SchemaSql
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import spock.lang.Shared
import spock.lang.Specification

class SqlQueryTemplatesDeriverSpec extends Specification {

    @Shared
    FilterEncoderSql filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, new CqlImpl(), null)
    @Shared
    SqlQueryTemplatesDeriver td = new SqlQueryTemplatesDeriver(null, filterEncoder, new SqlDialectPgis(), true, false, Optional.empty(), false)
    @Shared
    SqlQueryTemplatesDeriver tdNoNm = new SqlQueryTemplatesDeriver(null, filterEncoder, new SqlDialectPgis(), false, false, Optional.empty(), false)

    @Shared
    QuerySchemaDeriver schemaDeriver
    @Shared
    MappingOperationResolver mappingOperationResolver

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson()))
        schemaDeriver = new QuerySchemaDeriver(pathParser)
        mappingOperationResolver = new MappingOperationResolver()
    }

    static Optional<Cql2Expression> noFilter = Optional.empty()

    def 'meta query templates: #casename'() {

        when:

        SqlQueryTemplates templates = source.get(0).accept(deriver)
        String actual = meta(templates, sortBy, userFilter)

        then:

        actual == expected

        where:

        casename                      | deriver | sortBy                                                                            | userFilter | source                            || expected
        "basic"                       | td      | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META
        "basic without numberMatched" | tdNoNm  | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_WITHOUT_NUMBER_MATCHED
        "sortBy"                      | td      | [SortKey.of("created")]                                                           | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY
        "sortBy descending"           | td      | [SortKey.of("created", SortKey.Direction.DESCENDING)]                             | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY_DESC
        "sortBy mixed"                | td      | [SortKey.of("created", SortKey.Direction.DESCENDING), SortKey.of("lastModified")] | noFilter   | QuerySchemaFixtures.SIMPLE        || SqlQueryTemplatesFixtures.META_SORT_BY_MIXED
        "filter"                      | td      | []                                                                                | noFilter   | QuerySchemaFixtures.SIMPLE_FILTER || SqlQueryTemplatesFixtures.META_FILTER
    }

    def 'value query templates: #casename'() {

        when:

        SqlQueryTemplates templates = source.get(0).accept(deriver)
        List<String> actual = values(templates, limit, offset, sortBy, filter)

        then:

        actual == expected

        where:

        casename                             | deriver | limit | offset | sortBy                  | filter                                                                                                                      | source                                                  || expected
        "value array"                        | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.VALUE_ARRAY                         || SqlQueryTemplatesFixtures.VALUE_ARRAY
        "object array"                       | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY
        "merge"                              | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.MERGE                               || SqlQueryTemplatesFixtures.MERGE
        "self joins"                         | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.SELF_JOINS                          || SqlQueryTemplatesFixtures.SELF_JOINS
        "self joins with filters"            | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.SELF_JOINS_FILTER                   || SqlQueryTemplatesFixtures.SELF_JOINS_FILTER
        "self join with nested duplicate"    | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.SELF_JOIN_NESTED_DUPLICATE          || SqlQueryTemplatesFixtures.SELF_JOIN_NESTED_DUPLICATE
        "object without sourcePath"          | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.OBJECT_WITHOUT_SOURCE_PATH          || SqlQueryTemplatesFixtures.OBJECT_WITHOUT_SOURCE_PATH
        "paging"                             | td      | 10    | 10     | []                      | null                                                                                                                        | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY_PAGING
        "sortBy"                             | td      | 0     | 0      | [SortKey.of("created")] | null                                                                                                                        | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY_SORTBY
        "sortBy + filter"                    | td      | 0     | 0      | [SortKey.of("created")] | Eq.of(Property.of("task.title"), ScalarLiteral.of("foo"))                                                                   | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY_SORTBY_FILTER
        "sortBy + paging"                    | td      | 10    | 10     | [SortKey.of("created")] | null                                                                                                                        | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY_SORTBY_PAGING
        "sortBy + paging + filter"           | td      | 10    | 10     | [SortKey.of("created")] | And.of(Eq.of(Property.of("task.title"), ScalarLiteral.of("foo")), Eq.of(Property.of("task.href"), ScalarLiteral.of("bar"))) | QuerySchemaFixtures.OBJECT_ARRAY                        || SqlQueryTemplatesFixtures.OBJECT_ARRAY_SORTBY_PAGING_FILTER
        "property with multiple sourcePaths" | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.PROPERTY_WITH_MULTIPLE_SOURCE_PATHS || SqlQueryTemplatesFixtures.PROPERTY_WITH_MULTIPLE_SOURCE_PATHS
        "nested joins"                       | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.NESTED_JOINS                        || SqlQueryTemplatesFixtures.NESTED_JOINS
        "nested joins"                       | td      | 0     | 0      | []                      | null                                                                                                                        | QuerySchemaFixtures.NESTED_JOINS                        || SqlQueryTemplatesFixtures.NESTED_JOINS

    }

    def 'value query templates too: #casename'() {

        when:

        def sources = sqlSchemas(schema, schemaDeriver, mappingOperationResolver)
        List<SqlQueryTemplates> templates = sources.collect {it.accept(deriver) }
        List<String> actual = templates.collectMany { values(it, limit, offset, sortBy, filter) }
        List<String> expected = SqlQueryFixtures.fromYaml(queries)

        then:

        actual == expected

        where:

        casename                                      | deriver | limit | offset | sortBy | filter | schema                        || queries
        "self join with nested duplicate and filters" | td      | 0     | 0      | []     | null   | "okstra_abschnitt"            || "okstra_abschnitt"
        "embedded object with concat and backlink"    | td      | 0     | 0      | []     | null   | "pfs_plan-hatObjekt-embedded" || "pfs_plan-hatObjekt-embedded"
        "root concat with value concat with constant" | td      | 0     | 0      | []     | null   | "landcoverunit"               || "landcoverunit"

    }


    static String meta(SqlQueryTemplates templates, List<SortKey> sortBy, Optional<Cql2Expression> userFilter) {
        return templates.getMetaQueryTemplate().generateMetaQuery(10, 10, 0, sortBy, userFilter, ImmutableMap.of(), false, true)
    }

    static List<String> values(SqlQueryTemplates templates, int limit, int offset, List<SortKey> sortBy, Cql2Expression filter) {
        return templates.getValueQueryTemplates().collect { it.generateValueQuery(limit, offset, sortBy, Optional.ofNullable(filter), limit == 0 ? Optional.<Tuple<Object, Object>> empty() : Optional.of(Tuple.of(offset, offset + limit - 1)), ImmutableMap.of()) }
    }

    static List<SchemaSql> sqlSchemas(String featureSchemaName, QuerySchemaDeriver schemaDeriver, MappingOperationResolver mappingOperationResolver) {
        def schema = FeatureSchemaFixtures.fromYaml(featureSchemaName)
        def schema2 = schema.accept(new ConstantsResolver(), List.of()).accept(mappingOperationResolver, List.of())
        List<SchemaSql> sqlSchemas = schema2.accept(schemaDeriver)
        return sqlSchemas
    }

    static SchemaSql sqlSchema(String featureSchemaName, QuerySchemaDeriver schemaDeriver, MappingOperationResolver mappingOperationResolver) {
        return sqlSchemas(featureSchemaName, schemaDeriver, mappingOperationResolver).get(0)
    }

}
