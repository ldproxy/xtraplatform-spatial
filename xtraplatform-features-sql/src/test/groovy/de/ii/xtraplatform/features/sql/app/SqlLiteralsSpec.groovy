/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.base.app.EncryptionImpl
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.transform.PropertyEncryption
import spock.lang.Specification

class SqlLiteralsSpec extends Specification {

    def "string values are single-quoted with quote-doubling"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.STRING, value) == expected

        where:
        value               || expected
        "abc"               || "'abc'"
        "O'Brien"           || "'O''Brien'"
        "'; DROP TABLE x--" || "'''; DROP TABLE x--'"
        ""                  || "''"
    }

    def "integer values are re-rendered from a parsed number"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.INTEGER, value) == expected

        where:
        value  || expected
        "42"   || "42"
        "-7"   || "-7"
        "+7"   || "7"
        " 42 " || "42"
        "42.0" || "42"
        "1e3"  || "1000"
    }

    def "float values are re-rendered without scientific notation"() {
        expect:
        SqlLiterals.forType(SchemaBase.Type.FLOAT, value) == expected

        where:
        value    || expected
        "42"     || "42"
        "3.14"   || "3.14"
        "-0.5"   || "-0.5"
        "1.5e-3" || "0.0015"
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
        type                    | value
        SchemaBase.Type.INTEGER | "0 WHERE 1=1; DROP TABLE x --"
        SchemaBase.Type.INTEGER | "42; DELETE FROM t"
        SchemaBase.Type.INTEGER | "42.5"
        SchemaBase.Type.FLOAT   | "3.14 OR 1=1"
        SchemaBase.Type.FLOAT   | "NaN); DROP"
        SchemaBase.Type.BOOLEAN | "true; DROP TABLE x"
        SchemaBase.Type.INTEGER | ""
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

    def "values for encrypted columns are rejected by the plain literal renderer"() {
        when: 'a value for an encrypted column reaches forType'
        SqlLiterals.forType(type, "Mustermann")

        then: 'it is rejected instead of being inlined as plaintext'
        thrown(IllegalStateException)

        where:
        type << [SchemaBase.Type.ENCRYPTED, SchemaBase.Type.ENCRYPTED_ARRAY]
    }

    def "encrypted values are rendered as a bytea literal that decrypts to the plaintext"() {
        given: 'an encryption key'
        def key = (0..31).collect { it as byte } as byte[]
        def encryption = new PropertyEncryption(new EncryptionImpl(Base64.getEncoder().encodeToString(key)))

        when: 'a string value is rendered'
        def literal = SqlLiterals.encrypted(encryption, SchemaBase.Type.STRING, "O'Brien, Jürgen", 'nof')

        then: 'the literal is a quoted bytea hex value'
        literal.startsWith("'\\x")
        literal.endsWith("'")

        and: 'the hex value decrypts back to the plaintext'
        def bytes = java.util.HexFormat.of().parseHex(literal.substring(3, literal.length() - 1))
        encryption.decrypt(bytes, 'nof') == "O'Brien, Jürgen"
    }

    def "date values are normalized before encryption"() {
        given: 'an encryption key'
        def key = (0..31).collect { it as byte } as byte[]
        def encryption = new PropertyEncryption(new EncryptionImpl(Base64.getEncoder().encodeToString(key)))

        when: 'a canonical date value is rendered and decrypted'
        def literal = SqlLiterals.encrypted(encryption, SchemaBase.Type.DATE, '1957-06-30', 'geb')
        def bytes = java.util.HexFormat.of().parseHex(literal.substring(3, literal.length() - 1))

        then: 'the stored plaintext is the canonical ISO date'
        encryption.decrypt(bytes, 'geb') == '1957-06-30'

        when: 'a non-ISO date value is rendered'
        SqlLiterals.encrypted(encryption, SchemaBase.Type.DATE, '30.06.1957', 'geb')

        then: 'it is rejected as a client error'
        thrown(IllegalArgumentException)
    }

    def "a null value for an encrypted column is rendered as SQL NULL"() {
        given: 'an encryption key'
        def key = (0..31).collect { it as byte } as byte[]
        def encryption = new PropertyEncryption(new EncryptionImpl(Base64.getEncoder().encodeToString(key)))

        expect:
        SqlLiterals.encrypted(encryption, SchemaBase.Type.STRING, null, 'nof') == 'NULL'
    }
}
