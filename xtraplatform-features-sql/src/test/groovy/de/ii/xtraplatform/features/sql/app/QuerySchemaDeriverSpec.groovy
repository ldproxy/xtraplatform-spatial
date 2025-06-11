/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.ImmutableSqlPathSyntax
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SchemaSql
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import spock.lang.Shared
import spock.lang.Specification

class QuerySchemaDeriverSpec extends Specification {

    @Shared
    QuerySchemaDeriver schemaDeriver
    @Shared
    MutationSchemaDeriver schemaDeriver2
    @Shared
    MappingOperationResolver mappingOperationResolver

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson()))
        def pathParser2 = new PathParserSql(ImmutableSqlPathSyntax.builder().options(defaults).build(), cql)
        schemaDeriver = new QuerySchemaDeriver(pathParser)
        schemaDeriver2 = new MutationSchemaDeriver(pathParser2, pathParser)
        mappingOperationResolver = new MappingOperationResolver()
    }

    def 'query schema: #casename'() {

        when:

        List<SchemaSql> actual = source.accept(mappingOperationResolver, List.of()).accept(schemaDeriver)
        List<SchemaSql> actual2 = source.accept(mappingOperationResolver, List.of()).accept(schemaDeriver2)
        //TODO: no difference when no joins?
        SchemaSql actual3 = actual2.get(0).accept(new MutationSchemaBuilderSql());

        QuerySchemaFixtures.toYaml(actual, casename.replace(' ', '_'))
        QuerySchemaFixtures.toYaml(actual2, casename.replace(' ', '_') + "_m")
        //QuerySchemaFixtures.toYaml([actual3], casename.replace(' ', '_') + "_m2")

        def actual4 = QuerySchemaFixtures.toYamlRaw(actual)
        def expected = QuerySchemaFixtures.fromYamlRaw(target)

        def actual5 = QuerySchemaFixtures.toYamlRaw(actual2)

        then:

        actual4 == expected
        //actual5 == expected

        where:

        casename                                     | source                                                    || target
        "simple"                                     | FeatureSchemaFixtures.SIMPLE                              || "simple"
        "simple filter"                              | FeatureSchemaFixtures.SIMPLE_FILTER                       || "simple_filter"
        "value array"                                | FeatureSchemaFixtures.VALUE_ARRAY                         || "value_array"
        "object array"                               | FeatureSchemaFixtures.OBJECT_ARRAY                        || "object_array"
        "merge"                                      | FeatureSchemaFixtures.MERGE                               || "merge"
        "self joins"                                 | FeatureSchemaFixtures.SELF_JOINS                          || "self_joins"
        //"self joins with filters"              | FeatureSchemaFixtures.SELF_JOINS_FILTER                   || QuerySchemaFixtures.SELF_JOINS_FILTER
        "self join with nested duplicate join"       | FeatureSchemaFixtures.SELF_JOIN_NESTED_DUPLICATE          || "self_join_with_nested_duplicate_join"
        "object without sourcePath"                  | FeatureSchemaFixtures.OBJECT_WITHOUT_SOURCE_PATH          || "object_without_sourcePath"
        "object without sourcePath with nested join" | FeatureSchemaFixtures.OBJECT_WITHOUT_SOURCE_PATH2         || "object_without_sourcePath_with_nested_join"
        "multiple sourcePaths"                       | FeatureSchemaFixtures.PROPERTY_WITH_MULTIPLE_SOURCE_PATHS || "multiple_sourcePaths"
        "concat values with join"                    | FeatureSchemaFixtures.CONCAT_VALUES_JOIN                  || "concat_values_with_join"
        "nested joins"                               | FeatureSchemaFixtures.NESTED_JOINS                        || "nested_joins"
        "nested value array"                         | FeatureSchemaFixtures.NESTED_VALUE_ARRAY                  || "nested_value_array"
        "simple connector"                           | FeatureSchemaFixtures.CONNECTOR_SIMPLE                    || "simple_connector"
        "merge connector"                            | FeatureSchemaFixtures.CONNECTOR_MERGE                     || "merge_connector"
        "object connector"                           | FeatureSchemaFixtures.CONNECTOR_OBJECT                    || "object_connector"
        "merge object connector"                     | FeatureSchemaFixtures.CONNECTOR_MERGE_OBJECT              || "merge_object_connector"
        "concat object arrays"                       | FeatureSchemaFixtures.CONCAT_OBJECT_ARRAYS                || "concat_object_arrays"
        "concat root objects"                        | FeatureSchemaFixtures.CONCAT_ROOT                         || "concat_root_objects"
        "long value path overlap"                    | FeatureSchemaFixtures.fromYaml('gemeinde_bezeichnung')    || "long_value_path_overlap"
    }


}
