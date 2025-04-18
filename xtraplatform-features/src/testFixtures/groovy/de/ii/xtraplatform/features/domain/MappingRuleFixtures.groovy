/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import com.fasterxml.jackson.core.type.TypeReference

class MappingRuleFixtures {

    static String fromYamlRaw(String name) {
        return YamlSerialization.fromYamlRaw(name, "mapping-rules");
    }

    static String toYamlRaw(Object value) {
        return YamlSerialization.toYamlRaw(value);
    }

    static List<MappingRule> fromYaml(String name) {
        return YamlSerialization.fromYaml(
                new TypeReference<List<ImmutableMappingRule.Builder>>() {
                },
                name,
                "mapping-rules",
                (builderList) -> builderList.stream().map(builder -> builder.build()).toList());
    }

    static void toYaml(List<MappingRule> schema, String name) {
        YamlSerialization.toYaml(
                schema,
                name,
                "mapping-rules");
    }
}
