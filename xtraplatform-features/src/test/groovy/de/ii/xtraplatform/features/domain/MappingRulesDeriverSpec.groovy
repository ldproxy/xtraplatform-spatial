/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.SchemaBase.Type
import spock.lang.Shared
import spock.lang.Specification

class MappingRulesDeriverSpec extends Specification {

    @Shared
    MappingRulesDeriver deriver
    @Shared
    MappingOperationResolver mappingOperationResolver
    @Shared
    ConstantsResolver constantsResolver

    def setupSpec() {
        deriver = new MappingRulesDeriver()
        mappingOperationResolver = new MappingOperationResolver()
        constantsResolver = new ConstantsResolver()
    }

    def 'query schema: #casename'() {

        when:

        def schema = FeatureSchemaFixtures.fromYaml(source)
        def resolved = schema.accept(constantsResolver, List.of()).accept(mappingOperationResolver, List.of())
        List<MappingRule> rules = resolved.accept(deriver)
        String actual = MappingRuleFixtures.toYamlRaw(rules)

        def expected = MappingRuleFixtures.fromYamlRaw(target)

        then:

        actual == expected

        where:

        casename                                      | source                                       || target
        "simple"                                      | "simple"                                     || "simple"
        "simple filter"                               | "simple_filter"                              || "simple_filter"
        "simple filter scoped queryable"              | "simple_filter_scopes"                       || "simple_filter_scopes"
        "value array"                                 | "value_array"                                || "value_array"
        "object array"                                | "object_array"                               || "object_array"
        "join with sortKey"                           | "join_sortKey"                               || "join_sortKey"
        "merge"                                       | "merge"                                      || "merge"
        "self joins"                                  | "self_joins"                                 || "self_joins"
        "self joins with filters"                     | "self_joins_filter"                          || "self_joins_filter"
        "self join with nested duplicate join"        | "self_joins_with_nested_duplicate_join"      || "self_joins_with_nested_duplicate_join"
        "object without sourcePath"                   | "object_without_sourcePath"                  || "object_without_sourcePath"
        "object without sourcePath with nested join"  | "object_without_sourcePath_with_nested_join" || "object_without_sourcePath_with_nested_join"
        "concat values"                               | "concat_values"                              || "concat_values"
        "concat values with join"                     | "concat_values_with_join"                    || "concat_values_with_join"
        "nested joins"                                | "nested_joins"                               || "nested_joins"
        "nested value array"                          | "nested_value_array"                         || "nested_value_array"
        "simple connector"                            | "simple_connector"                           || "simple_connector"
        "merge connector"                             | "merge_connector"                            || "merge_connector"
        "object connector"                            | "object_connector"                           || "object_connector"
        "merge object connector"                      | "merge_object_connector"                     || "merge_object_connector"
        "concat object arrays"                        | "pfs_plan-hatObjekt-2"                       || "pfs_plan-hatObjekt"
        "concat root objects"                         | "concat_root_objects"                        || "concat_root_objects"
        "long value path overlap"                     | "gemeinde_bezeichnung"                       || "long_value_path_overlap"
        "self join with nested duplicate and filters" | "okstra_abschnitt"                           || "okstra_abschnitt"
        "embedded object with concat and backlink"    | "pfs_plan-hatObjekt-embedded"                || "pfs_plan-hatObjekt-embedded"
        "root concat with value concat with constant" | "landcoverunit"                              || "landcoverunit"
        "strassen_unfaelle2"                          | "strassen_unfaelle2"                         || "strassen_unfaelle2"
        "simple expression"                           | "simple_expression"                          || "simple_expression"
        "bare VALUE resolves to leaf type"            | "value_ref"                                  || "value_ref"
    }

    def 'bare Type.VALUE is resolved to its valueType (default STRING) in the emitted MappingRule'() {
        // regression: a property declared as `type: VALUE, valueType: STRING` (used by xlink-reference
        // properties in NAS/ALKIS provider configs) was previously emitting MappingRule.type=VALUE,
        // which then propagated to SqlQueryColumn.type=VALUE — and FeatureEncoderSql.onValue only
        // quotes STRING/DATETIME/DATE, so inserts produced unquoted SQL literals and Postgres parsed
        // them as identifiers. MappingRule has no valueType field, so the resolution has to happen here.

        given:
        def schema = new ImmutableFeatureSchema.Builder()
                .name('flurstueck')
                .sourcePath('/flurstueck')
                .type(Type.OBJECT)
                .putProperties2('rel_buchung', new ImmutableFeatureSchema.Builder()
                        .sourcePath('p_buchung')
                        .type(Type.VALUE)
                        .valueType(Type.STRING))
                .putProperties2('rel_default', new ImmutableFeatureSchema.Builder()
                        .sourcePath('p_default')
                        .type(Type.VALUE))

        when:
        List<MappingRule> rules = schema.build().accept(deriver)
        def byTarget = rules.collectEntries { [(it.target): it] }

        then: 'explicit valueType wins'
        byTarget['rel_buchung'].type == Type.STRING

        and: 'missing valueType defaults to STRING (mirrors the existing VALUE_ARRAY handling)'
        byTarget['rel_default'].type == Type.STRING

        and: 'Type.VALUE never appears in emitted rules'
        rules.every { it.type != Type.VALUE }
    }


}
