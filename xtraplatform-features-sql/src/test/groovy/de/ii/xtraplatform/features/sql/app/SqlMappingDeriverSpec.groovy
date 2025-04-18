/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema
import spock.lang.Shared
import spock.lang.Specification

class SqlMappingDeriverSpec extends Specification {

    @Shared
    SqlMappingDeriver deriver

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        //def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson()))

        deriver = new SqlMappingDeriver(pathParser)
    }

    def 'sql mapping rules: #casename'() {

        when:

        def mappingRules = MappingRuleFixtures.fromYaml(rules)
        SqlQueryMapping mapping = deriver.derive(mappingRules)
        List<SqlQuerySchema> schemas = mapping.getTables()
        String actual = SqlQuerySchemaFixtures.toYamlRaw(schemas)
        String expected = SqlQuerySchemaFixtures.fromYamlRaw(schema)

        then:

        actual == expected

        where:

        //TODO: column ops
        casename                                      | rules                                   || schema
        "simple"                                      | "simple"                                || "simple"
        "simple filter"                               | "simple_filter"                         || "simple_filter"
        "value array"                                 | "value_array"                           || "value_array"
        "object array"                                | "object_array"                          || "object_array"
        "merge"                                       | "merge"                                 || "merge"
        "self joins"                                  | "self_joins"                            || "self_joins"
        "self joins with filters"                     | "self_joins_filter"                     || "self_joins_filter"
        "self joins with nested duplicate"            | "self_joins_with_nested_duplicate_join" || "self_joins_with_nested_duplicate_join"
        "object without sourcePath"                   | "object_without_sourcePath"             || "object_without_sourcePath"
        "property with multiple sourcePaths"          | "concat_values"                         || "property_with_multiple_sourcePaths"
        "nested joins"                                | "nested_joins"                          || "nested_joins"
        //"nested value array"                         | FeatureSchemaFixtures.NESTED_VALUE_ARRAY || "nested_value_array"
        "simple connector"                            | "simple_connector"                      || "connector"
        "merge connector"                             | "merge_connector"                       || "connector"
        "object connector"                            | "object_connector"                      || "connector"
        "merge object connector"                      | "merge_object_connector"                || "connector"
        "self join with nested duplicate and filters" | "okstra_abschnitt"                      || "okstra_abschnitt"
        "embedded object with concat and backlink"    | "pfs_plan-hatObjekt-embedded"           || "pfs_plan-hatObjekt-embedded"
        "root concat with value concat with constant" | "landcoverunit"                         || "landcoverunit"
    }
}
