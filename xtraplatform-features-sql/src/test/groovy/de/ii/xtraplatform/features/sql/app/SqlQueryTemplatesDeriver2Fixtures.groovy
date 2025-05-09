/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import com.fasterxml.jackson.core.type.TypeReference
import de.ii.xtraplatform.features.domain.YamlSerialization
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema

class SqlQueryTemplatesDeriver2Fixtures {

    static String fromYamlRaw(String name) {
        return YamlSerialization.fromYamlRaw(name, "sql-query-schemas");
    }

    static String toYamlRaw(Object value) {
        return YamlSerialization.toYamlRaw(value);
    }

    static List<SqlQuerySchema> fromYaml(String name) {
        return YamlSerialization.fromYaml(
                new TypeReference<List<ImmutableSqlQuerySchema.Builder>>() {
                },
                name,
                "sql-query-schemas",
                (builderList) -> builderList.stream().map(builder -> builder.build()).toList());
    }

    static void toYaml(List<SqlQuerySchema> schema, String name) {
        YamlSerialization.toYaml(
                schema,
                name,
                "sql-query-schemas");
    }
}
