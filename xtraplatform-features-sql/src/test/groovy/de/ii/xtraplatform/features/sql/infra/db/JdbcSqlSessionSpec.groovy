/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import spock.lang.Specification

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * Locks the JDBC-level session that backs every {@code FeatureTransactions.Session}: it owns the
 * connection's auto-commit lifecycle, dispatches generated ids back to per-statement consumers,
 * and guarantees idempotent commit/rollback/close semantics.
 *
 * <p>Why this matters: the executor in ogcapi-transactions relies on rollback being safe to call
 * after a failed action, and on close() not double-rolling back a session that already committed.
 * A regression here would silently leak connections or roll back already-committed work.
 */
class JdbcSqlSessionSpec extends Specification {

    Connection connection
    Statement statement
    ResultSet resultSet

    def setup() {
        connection = Mock(Connection)
        statement = Mock(Statement)
        resultSet = Mock(ResultSet)
        connection.isClosed() >> false
        connection.createStatement() >> statement
    }

    def 'constructor flips autoCommit off so all subsequent statements join one transaction'() {
        when:
        new JdbcSqlSession(connection)

        then:
        1 * connection.setAutoCommit(false)
    }

    def 'constructor failure (setAutoCommit throws) closes the connection and surfaces the cause'() {
        given:
        connection.setAutoCommit(false) >> { throw new SQLException('boom') }

        when:
        new JdbcSqlSession(connection)

        then:
        thrown(IllegalStateException)
        1 * connection.close()
    }

    def 'run executes each non-null statement, captures RETURNING id, dispatches to per-statement consumers'() {
        given:
        def session = new JdbcSqlSession(connection)
        def captured = []
        Consumer<String> cons1 = { captured << ['s1', it] }
        Consumer<String> cons2 = { captured << ['s2', it] }
        statement.execute('INSERT 1 RETURNING id') >> true
        statement.execute('INSERT 2 RETURNING id') >> true
        statement.getResultSet() >>> [resultSet, resultSet]
        resultSet.next() >>> [true, true]
        resultSet.getString(1) >>> ['gen-1', 'gen-2']

        when:
        def returnedId = session.run(
                [{ 'INSERT 1 RETURNING id' } as Supplier<String>,
                 { 'INSERT 2 RETURNING id' } as Supplier<String>],
                [cons1, cons2],
                Optional.empty())

        then:
        captured == [['s1', 'gen-1'], ['s2', 'gen-2']]

        and: 'with no caller-supplied id the first generated id is returned'
        returnedId == 'gen-1'
    }

    def 'run returns the caller-supplied featureId when present, even if the DB generated different ids'() {
        given:
        def session = new JdbcSqlSession(connection)
        statement.execute(_ as String) >> true
        statement.getResultSet() >> resultSet
        resultSet.next() >> true
        resultSet.getString(1) >> 'gen-42'

        when:
        def id = session.run(
                [{ 'INSERT ... RETURNING id' } as Supplier<String>],
                [{ String s -> } as Consumer<String>],
                Optional.of('caller-7'))

        then:
        id == 'caller-7'
    }

    def 'run skips statements that evaluate to null (Supplier returns null)'() {
        given:
        def session = new JdbcSqlSession(connection)

        when:
        session.run(
                [{ (String) null } as Supplier<String>],
                [{ String s -> } as Consumer<String>],
                Optional.empty())

        then: 'no statement is ever executed'
        0 * connection.createStatement()
    }

    def 'run wraps SQLException from statement.execute as IllegalStateException with statement text'() {
        given:
        def session = new JdbcSqlSession(connection)
        statement.execute('BAD SQL') >> { throw new SQLException('syntax') }

        when:
        session.run(
                [{ 'BAD SQL' } as Supplier<String>],
                [{ String s -> } as Consumer<String>],
                Optional.empty())

        then:
        def ex = thrown(IllegalStateException)
        ex.message.contains('BAD SQL')
        ex.message.contains('syntax')
    }

    def 'consecutive RETURNING null statements are batched and consumers are called with null'() {
        given:
        def session = new JdbcSqlSession(connection)
        def captured = []
        Consumer<String> c1 = { captured << ['c1', it] }
        Consumer<String> c2 = { captured << ['c2', it] }

        when:
        session.run(
                [{ 'INSERT INTO child1 VALUES (...) RETURNING null;' } as Supplier<String>,
                 { 'INSERT INTO child2 VALUES (...) RETURNING null;' } as Supplier<String>],
                [c1, c2],
                Optional.empty())

        then: 'a single batch is executed, not two individual executes'
        1 * statement.addBatch('INSERT INTO child1 VALUES (...) RETURNING null;')
        1 * statement.addBatch('INSERT INTO child2 VALUES (...) RETURNING null;')
        1 * statement.executeBatch()
        0 * statement.execute(_ as String)

        and: 'both consumers receive null'
        captured == [['c1', null], ['c2', null]]
    }

    def 'a non-batchable statement flushes the pending batch first, then runs individually'() {
        given:
        def session = new JdbcSqlSession(connection)
        statement.execute('UPDATE feat SET x=1 RETURNING id;') >> true
        statement.getResultSet() >> resultSet
        resultSet.next() >> true
        resultSet.getString(1) >> 'gen-9'

        when:
        session.run(
                [{ 'INSERT INTO child VALUES (...) RETURNING null;' } as Supplier<String>,
                 { 'UPDATE feat SET x=1 RETURNING id;' } as Supplier<String>],
                [{ String s -> } as Consumer<String>,
                 { String s -> } as Consumer<String>],
                Optional.empty())

        then: 'pending batch runs first, then the non-batchable statement runs individually'
        1 * statement.addBatch('INSERT INTO child VALUES (...) RETURNING null;')
        1 * statement.executeBatch()
        1 * statement.execute('UPDATE feat SET x=1 RETURNING id;')
    }

    def 'batch is flushed at end of run even if last statement is batched'() {
        given:
        def session = new JdbcSqlSession(connection)

        when:
        session.run(
                [{ 'INSERT INTO child VALUES (...) RETURNING null;' } as Supplier<String>],
                [{ String s -> } as Consumer<String>],
                Optional.empty())

        then:
        1 * statement.addBatch('INSERT INTO child VALUES (...) RETURNING null;')
        1 * statement.executeBatch()
    }

    def 'RETURNING null detection ignores trailing semicolon and is case-insensitive'() {
        given:
        def session = new JdbcSqlSession(connection)

        when:
        session.run(
                [{ 'INSERT INTO a VALUES (1) returning NULL  ' } as Supplier<String>,
                 { 'INSERT INTO b VALUES (2) RETURNING NULL;' } as Supplier<String>],
                [{ String s -> } as Consumer<String>,
                 { String s -> } as Consumer<String>],
                Optional.empty())

        then:
        1 * statement.addBatch('INSERT INTO a VALUES (1) returning NULL  ')
        1 * statement.addBatch('INSERT INTO b VALUES (2) RETURNING NULL;')
        1 * statement.executeBatch()
        0 * statement.execute(_ as String)
    }

    def 'SQLException during executeBatch is wrapped as IllegalStateException with the first statement text'() {
        given:
        def session = new JdbcSqlSession(connection)
        statement.executeBatch() >> { throw new SQLException('constraint violated') }

        when:
        session.run(
                [{ 'INSERT INTO child VALUES (1) RETURNING null;' } as Supplier<String>],
                [{ String s -> } as Consumer<String>],
                Optional.empty())

        then:
        def ex = thrown(IllegalStateException)
        ex.message.contains('INSERT INTO child VALUES (1) RETURNING null;')
        ex.message.contains('constraint violated')
    }

    def 'commit calls connection.commit and releases the connection'() {
        given:
        def session = new JdbcSqlSession(connection)

        when:
        session.commit()

        then:
        1 * connection.commit()
        1 * connection.setAutoCommit(true)
        1 * connection.close()
    }

    def 'commit after finalisation throws (no double commit)'() {
        given:
        def session = new JdbcSqlSession(connection)
        session.commit()

        when:
        session.commit()

        then:
        thrown(IllegalStateException)
    }

    def 'rollback is idempotent after commit (no rollback is issued on the connection)'() {
        given:
        def session = new JdbcSqlSession(connection)
        session.commit()

        when:
        session.rollback()

        then:
        0 * connection.rollback()
    }

    def 'close() before commit rolls back, then releases the connection'() {
        given:
        def session = new JdbcSqlSession(connection)

        when:
        session.close()

        then:
        1 * connection.rollback()
        0 * connection.commit()
        // connection.close() is called via releaseConnection (once)
        (1.._) * connection.close()
    }

    def 'close() after commit does not roll back; it just releases the connection'() {
        given:
        def session = new JdbcSqlSession(connection)
        session.commit()

        when:
        session.close()

        then:
        0 * connection.rollback()
    }

    def 'rollback swallows SQLException from connection.rollback (still finalises)'() {
        given:
        def session = new JdbcSqlSession(connection)
        connection.rollback() >> { throw new SQLException('rolling-back-failed') }

        when:
        session.rollback()

        then: 'no exception bubbles out'
        noExceptionThrown()

        and: 'subsequent rollback is a no-op'
        session.rollback()
        1 * connection.rollback()  // only the original attempt
    }
}
