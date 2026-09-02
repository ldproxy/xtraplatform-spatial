/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryOptions
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter
import de.ii.xtraplatform.features.sql.domain.SqlDialect
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions
import de.ii.xtraplatform.features.sql.domain.SqlRow
import de.ii.xtraplatform.streams.app.SourceDefault
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.subscribers.TestSubscriber
import spock.lang.Specification

import javax.sql.DataSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.CompletionException

/**
 * Locks the connection lifecycle of {@link SqlClientRx}: every connection is leased from the pool
 * when it is needed and returned in every case - rows exhausted, statement failed, consumer
 * cancelled - so the pool cannot be drained by a read or a statement that ends abnormally.
 */
class SqlClientRxSpec extends Specification {

    static final SqlQueryOptions PAGED = SqlQueryOptions.withColumnTypes(String.class)
    static final SqlQueryOptions STREAMED =
            new ImmutableSqlQueryOptions.Builder().from(PAGED).fetchSize(2).build()

    DataSource dataSource
    Connection connection
    Statement statement
    ResultSet resultSet
    boolean leaseFails
    boolean executeFails

    SqlClientRx sqlClient

    def setup() {
        dataSource = Mock(DataSource)
        connection = Mock(Connection)
        statement = Mock(Statement)
        resultSet = Mock(ResultSet)
        leaseFails = false
        executeFails = false
        dataSource.getConnection() >> {
            if (leaseFails) throw new SQLException('Connection is not available, request timed out after 30000ms')
            return connection
        }
        connection.createStatement() >> statement
        statement.executeQuery(_ as String) >> {
            if (executeFails) throw new SQLException('relation does not exist', '42P01')
            return resultSet
        }
        resultSet.getStatement() >> statement

        sqlClient = new SqlClientRx(dataSource, Mock(SqlDbmsAdapter), Mock(SqlDialect), Optional.empty())
    }

    def 'a streamed read runs in a transaction, closes statement and result set and returns the connection'() {
        given:
        rows('a', 'b')

        when:
        TestSubscriber<SqlRow> subscriber = read('SELECT id FROM t', STREAMED)

        then:
        subscriber.assertValueCount(2)
        subscriber.assertComplete()
        1 * connection.setAutoCommit(false)
        1 * statement.setFetchSize(2)
        1 * resultSet.close()
        1 * statement.close()
        1 * connection.rollback()
        1 * connection.setAutoCommit(true)
        1 * connection.close()
    }

    def 'a paged read runs with autocommit and returns the connection'() {
        given:
        rows('a')

        when:
        TestSubscriber<SqlRow> subscriber = read('SELECT id FROM t', PAGED)

        then:
        subscriber.assertValueCount(1)
        subscriber.assertComplete()
        0 * connection.setAutoCommit(_)
        0 * statement.setFetchSize(_)
        0 * connection.rollback()
        1 * connection.close()
    }

    def 'a read cancelled by the consumer returns the connection'() {
        given:
        rows('a', 'b', 'c')

        when:
        TestSubscriber<SqlRow> subscriber = Flowable.fromPublisher(publisher('SELECT id FROM t', STREAMED)).take(1).test()

        then:
        subscriber.assertValueCount(1)
        subscriber.assertComplete()
        1 * resultSet.close()
        1 * connection.rollback()
        1 * connection.close()
    }

    def 'an error while reading rows is delivered and the connection is returned'() {
        given:
        resultSet.next() >> { throw new SQLException('terminating connection due to administrator command', '57P01') }

        when:
        TestSubscriber<SqlRow> subscriber = read('SELECT id FROM t', STREAMED)

        then:
        subscriber.assertError(SQLException)
        1 * resultSet.close()
        1 * connection.close()
    }

    def 'a statement that fails to execute is delivered as an error and the connection is returned'() {
        given:
        executeFails = true

        when:
        TestSubscriber<SqlRow> subscriber = read('SELECT id FROM missing', STREAMED)

        then:
        subscriber.assertError(SQLException)
        1 * statement.close()
        1 * connection.close()
    }

    def 'a connection that cannot be leased for a read fails the read'() {
        given:
        leaseFails = true

        when:
        TestSubscriber<SqlRow> subscriber = read('SELECT id FROM t', PAGED)

        then:
        subscriber.assertError(SQLException)
        0 * connection.close()
    }

    def 'run reads all rows and returns the connection'() {
        given:
        rows('a', 'b', 'c')

        when:
        def result = sqlClient.run('SELECT id FROM t', PAGED).join()

        then:
        result.size() == 3
        1 * resultSet.close()
        1 * statement.close()
        1 * connection.close()
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

    private void rows(String... ids) {
        List<Boolean> next = ids.collect { true } + [false]
        resultSet.next() >>> next
        resultSet.getString(1) >>> (ids as List)
    }

    private TestSubscriber<SqlRow> read(String sql, SqlQueryOptions options) {
        return Flowable.fromPublisher(publisher(sql, options)).test()
    }

    private org.reactivestreams.Publisher<SqlRow> publisher(String sql, SqlQueryOptions options) {
        return ((SourceDefault<SqlRow>) sqlClient.getSourceStream(sql, options)).getPublisher()
    }
}
