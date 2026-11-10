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
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.ImmutableSqlPathSyntax
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
@Ignore
class MutationSchemaDeriverSpec extends Specification {

    @Shared
            pathParser, pathParser2, schemaBuilderSql

    def setupSpec() {
        def syntax = ImmutableSqlPathSyntax.builder().build()
        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()

        pathParser = new PathParserSql(syntax)
        pathParser2 = new SqlPathParser(defaults, cql, Set.of())
        schemaBuilderSql = new MutationSchemaDeriver(pathParser, pathParser2)
    }

    def 'mutation schema: #casename'() {

        when:

        def actual = source.accept(schemaBuilderSql)

        then:

        actual == expected

        where:

        casename       | source                             || expected
        "value array"  | FeatureSchemaFixtures.VALUE_ARRAY || MutationSchemaFixtures.VALUE_ARRAY
        "object array" | FeatureSchemaFixtures.OBJECT_ARRAY || MutationSchemaFixtures.OBJECT_ARRAY
    }

}
