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
import de.ii.xtraplatform.streams.app.SourceDefault
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.subscribers.TestSubscriber
import org.davidmoten.rxjava3.jdbc.ConnectionProvider
import org.davidmoten.rxjava3.jdbc.Database
import spock.lang.Specification

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * Locks the connection lifecycle of the rxjava3-jdbc mutation chain built by
 * {@link SqlClientRx#getMutationSource}.
 *
 * <p>Why this matters: the transacted connection is reference counted and is only really rolled
 * back and returned to the pool when the count reaches 0. A statement stream that is cancelled
 * instead of terminated never releases its reference, and that is exactly what happens to all
 * preceding statements when a later statement of the chain fails. Without the guard, every failed
 * CREATE/PUT/PATCH whose error occurs after the first statement permanently leaks a pooled
 * connection until the pool is starved (#1711).
 */
class SqlClientRxSpec extends Specification {

    Connection connection
    Map<String, PreparedStatement> preparedStatements
    boolean closed

    SqlClientRx sqlClient

    def setup() {
        preparedStatements = [:]
        closed = false

        connection = Mock(Connection)
        connection.getAutoCommit() >> false
        connection.isClosed() >> { closed }
        connection.prepareStatement(_ as String, Statement.RETURN_GENERATED_KEYS) >> { String sql, int keys ->
            preparedStatements.get(sql)
        }

        Database database = Database.fromBlocking(new ConnectionProvider() {
            @Override
            Connection get() {
                return connection
            }

            @Override
            void close() {
            }
        })

        sqlClient = new SqlClientRx(database, Mock(SqlDbmsAdapter), Mock(SqlDialect), Optional.empty())
    }

    def 'a failure in the second statement rolls back and releases the connection'() {
        given:
        statement('INSERT 1', 'id1')
        failingStatement('INSERT 2')

        when:
        TestSubscriber<String> subscriber = subscribe(['INSERT 1', 'INSERT 2'])

        then:
        subscriber.assertError(SQLException)
        1 * connection.rollback()
        1 * connection.close() >> { closed = true }
        0 * connection.commit()
    }

    def 'a failure in the third statement releases both outstanding references'() {
        given:
        statement('INSERT 1', 'id1')
        statement('INSERT 2', 'id2')
        failingStatement('INSERT 3')

        when:
        TestSubscriber<String> subscriber = subscribe(['INSERT 1', 'INSERT 2', 'INSERT 3'])

        then:
        subscriber.assertError(SQLException)
        1 * connection.rollback()
        1 * connection.close() >> { closed = true }
        0 * connection.commit()
    }

    def 'a successful chain still commits exactly once and is not rolled back'() {
        given:
        statement('INSERT 1', 'id1')
        statement('INSERT 2', 'id2')

        when:
        TestSubscriber<String> subscriber = subscribe(['INSERT 1', 'INSERT 2'])

        then:
        subscriber.assertComplete()
        1 * connection.commit()
        1 * connection.close() >> { closed = true }
        0 * connection.rollback()
    }

    def 'cancellation by the consumer rolls back and releases the connection'() {
        given:
        statement('INSERT 1', 'id1')
        statement('INSERT 2', 'id2')

        when:
        cancelAfterFirst(['INSERT 1', 'INSERT 2'])

        then:
        1 * connection.rollback()
        1 * connection.close() >> { closed = true }
        0 * connection.commit()
    }

    def 'each id consumer receives the id returned by its own statement'() {
        given:
        statement('INSERT 1', 'id1')
        statement('INSERT 2', 'id2')
        statement('INSERT 3', 'id3')

        and:
        List<String> received = [null, null, null]
        List<Consumer<String>> idConsumers = (0..2).collect { int index ->
            ({ String id -> received.set(index, id) } as Consumer<String>)
        }

        when:
        TestSubscriber<String> subscriber = subscribe(['INSERT 1', 'INSERT 2', 'INSERT 3'], idConsumers)

        then:
        subscriber.assertComplete()
        received == ['id1', 'id2', 'id3']
        1 * connection.close() >> { closed = true }
    }

    def 'a missing id consumer does not fail the mutation'() {
        given:
        statement('INSERT 1', 'id1')
        statement('INSERT 2', 'id2')

        when:
        TestSubscriber<String> subscriber = subscribe(['INSERT 1', 'INSERT 2'], [null, null])

        then:
        subscriber.assertComplete()
        1 * connection.commit()
        1 * connection.close() >> { closed = true }
        0 * connection.rollback()
    }

    private void statement(String sql, String id) {
        PreparedStatement preparedStatement = Mock(PreparedStatement)
        ResultSet resultSet = Mock(ResultSet)

        preparedStatement.execute() >> true
        preparedStatement.getGeneratedKeys() >> resultSet
        resultSet.next() >>> [true, false]
        resultSet.getString(1) >> id

        preparedStatements.put(sql, preparedStatement)
    }

    private void failingStatement(String sql) {
        PreparedStatement preparedStatement = Mock(PreparedStatement)

        preparedStatement.execute() >> {
            throw new SQLException('duplicate key value violates unique constraint')
        }

        preparedStatements.put(sql, preparedStatement)
    }

    private TestSubscriber<String> subscribe(List<String> sql, List<Consumer<String>> idConsumers = null) {
        return Flowable.fromPublisher(publisher(sql, idConsumers)).test()
    }

    private void cancelAfterFirst(List<String> sql) {
        Flowable.fromPublisher(publisher(sql, null)).take(1).test()
    }

    private org.reactivestreams.Publisher<String> publisher(List<String> sql, List<Consumer<String>> idConsumers) {
        List<Supplier<String>> statements = sql.collect { String s -> ({ -> s } as Supplier<String>) }
        List<Consumer<String>> consumers = idConsumers ?: sql.collect { ({ String id -> } as Consumer<String>) }

        SourceDefault<String> source =
                (SourceDefault<String>) sqlClient.getMutationSource(statements, consumers, null, Optional.empty())

        return source.getPublisher()
    }
}
