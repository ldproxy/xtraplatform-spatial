/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import java.sql.SQLException
import org.postgresql.util.PSQLException
import org.postgresql.util.ServerErrorMessage
import spock.lang.Specification
import spock.lang.Unroll

/**
 * A failed feature query: a data exception (SQLSTATE class 22) is caused by a value in the
 * request and becomes an IllegalArgumentException (400), every other database error stays a
 * server error with the details of the server error message.
 */
class SqlQueryErrorSpec extends Specification {

    static PSQLException serverError(String sqlState, String message) {
        new PSQLException(new ServerErrorMessage("SERROR\u0000C${sqlState}\u0000M${message}\u0000P377\u0000"))
    }

    @Unroll
    def "data exception #sqlState is a client error"() {
        given:
        PSQLException error = serverError(sqlState, message)

        when:
        Throwable mapped = SqlConnector.PSQL_CONTEXT.apply(error)

        then:
        mapped instanceof IllegalArgumentException
        mapped.message == "A value in the request is not valid for the queried data: " + message
        mapped.cause.is(error)

        where:
        sqlState | message
        "22P02"  | 'invalid input syntax for type uuid: "AAA"'
        "22003"  | 'value "2147483648" is out of range for type integer'
        "22007"  | 'invalid input syntax for type timestamp: "x"'
        "22008"  | 'date/time field value out of range: "2024-13-45"'
    }

    def "data exception without server error message uses the exception message"() {
        given:
        SQLException error = new SQLException("Conversion Error", "22018")

        when:
        Throwable mapped = SqlConnector.PSQL_CONTEXT.apply(error)

        then:
        mapped instanceof IllegalArgumentException
        mapped.message == "A value in the request is not valid for the queried data: Conversion Error"
    }

    @Unroll
    def "error #sqlState stays a server error"() {
        given:
        PSQLException error = serverError(sqlState, "some error")

        when:
        Throwable mapped = SqlConnector.PSQL_CONTEXT.apply(error)

        then:
        mapped instanceof PSQLException
        mapped.message.startsWith("Unexpected SQL query error: ")
        mapped.message.contains("some error")

        where:
        sqlState << ["42P01", "42703", "08006", "53300", "57014", "XX000"]
    }

    def "other errors are passed through"() {
        given:
        Throwable error = new IllegalStateException("x")

        expect:
        SqlConnector.PSQL_CONTEXT.apply(error).is(error)
    }
}
