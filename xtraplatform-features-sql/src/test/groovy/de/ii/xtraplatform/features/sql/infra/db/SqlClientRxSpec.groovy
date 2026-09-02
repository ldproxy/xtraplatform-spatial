/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter
import de.ii.xtraplatform.features.sql.domain.SqlDialect
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions
import org.davidmoten.rxjava3.jdbc.ConnectionProvider
import org.davidmoten.rxjava3.jdbc.Database
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.CompletionException

/**
 * Locks the connection lifecycle of everything in {@link SqlClientRx} that is not a streamed read:
 * a connection is leased from the pool directly and returned to it in every case, so no code path
 * outside the reads depends on the rxjava3-jdbc reference counting.
 */
class SqlClientRxSpec extends Specification {

    DataSource dataSource
    Connection connection
    Statement statement
    boolean leaseFails

    SqlClientRx sqlClient

    def setup() {
        dataSource = Mock(DataSource)
        connection = Mock(Connection)
        statement = Mock(Statement)
        leaseFails = false
        dataSource.getConnection() >> {
            if (leaseFails) throw new SQLException('Connection is not available, request timed out after 30000ms')
            return connection
        }
        connection.createStatement() >> statement

        Database database = Database.fromBlocking(new ConnectionProvider() {
            @Override
            Connection get() { return connection }

            @Override
            void close() {}
        })

        sqlClient = new SqlClientRx(database, dataSource, Mock(SqlDbmsAdapter), Mock(SqlDialect), Optional.empty())
    }

    def 'a statement without a result runs on a pooled connection that is closed afterwards'() {
        when:
        def result = sqlClient.run('CREATE TABLE t (id int)', SqlQueryOptions.ddl()).join()

        then:
        result.isEmpty()
        1 * statement.execute('CREATE TABLE t (id int)') >> false
        1 * statement.close()
        1 * connection.close()
    }

    def 'a failing statement without a result surfaces the error and still closes the connection'() {
        given:
        statement.execute(_ as String) >> { throw new SQLException('relation does not exist', '42P01') }

        when:
        sqlClient.run('DROP TABLE t', SqlQueryOptions.ddl()).join()

        then:
        def e = thrown(CompletionException)
        e.cause instanceof SQLException
        1 * connection.close()
    }

    def 'a session leases its connection from the pool and returns it on close'() {
        when:
        def session = sqlClient.openSession()
        session.close()

        then:
        1 * dataSource.getConnection() >> connection
        1 * connection.setAutoCommit(false)
        1 * connection.close()
    }

    def 'a connection that cannot be leased is reported with its cause'() {
        given:
        leaseFails = true

        when:
        sqlClient.getConnection()

        then:
        def e = thrown(IllegalStateException)
        e.cause instanceof SQLException
        e.message.contains('not available')
    }
}
