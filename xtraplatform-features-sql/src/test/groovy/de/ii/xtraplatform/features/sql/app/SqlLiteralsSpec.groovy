/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.SchemaBase
import spock.lang.Specification

class SqlLiteralsSpec extends Specification {

    def "string values are single-quoted with quote-doubling"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.STRING, value) == expected

        where:
        value              || expected
        "abc"              || "'abc'"
        "O'Brien"          || "'O''Brien'"
        "'; DROP TABLE x--" || "'''; DROP TABLE x--'"
        ""                 || "''"
    }

    def "integer values are re-rendered from a parsed number"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.INTEGER, value) == expected

        where:
        value    || expected
        "42"     || "42"
        "-7"     || "-7"
        "+7"     || "7"
        " 42 "   || "42"
        "42.0"   || "42"
        "1e3"    || "1000"
    }

    def "float values are re-rendered without scientific notation"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.FLOAT, value) == expected

        where:
        value      || expected
        "42"       || "42"
        "3.14"     || "3.14"
        "-0.5"     || "-0.5"
        "1.5e-3"   || "0.0015"
    }

    def "boolean values are normalized to the SQL keywords"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.BOOLEAN, value) == expected

        where:
        value   || expected
        "true"  || "TRUE"
        "TRUE"  || "TRUE"
        "1"     || "TRUE"
        "false" || "FALSE"
        "0"     || "FALSE"
    }

    def "injection attempts through a numeric column are rejected, not inlined"() {
        when:
        SqlLiterals.forType(type, value)

        then:
        thrown(IllegalArgumentException)

        where:
        type                     | value
        SchemaBase.Type.INTEGER  | "0 WHERE 1=1; DROP TABLE x --"
        SchemaBase.Type.INTEGER  | "42; DELETE FROM t"
        SchemaBase.Type.INTEGER  | "42.5"
        SchemaBase.Type.FLOAT    | "3.14 OR 1=1"
        SchemaBase.Type.FLOAT    | "NaN); DROP"
        SchemaBase.Type.BOOLEAN  | "true; DROP TABLE x"
        SchemaBase.Type.INTEGER  | ""
    }

    def "null values become the SQL NULL keyword"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.INTEGER, null) == "NULL"
        SqlLiterals.forType(SchemaBase.Type.STRING, null) == "NULL"
    }

    def "unmapped column types fall back to a quoted literal rather than raw inlining"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.FEATURE_REF, "abc'; DROP") == "'abc''; DROP'"
    }
}
