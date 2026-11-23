/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter
import de.ii.xtraplatform.features.sql.domain.SqlDialect
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import javax.sql.DataSource
import java.sql.Connection
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Locks how a connection that SchemaCrawler has worked on is released. SchemaCrawler sets the
 * schema of the connection while crawling, which overwrites the search path of a PostgreSQL
 * connection; returned to the pool, the connection would serve later queries with that search
 * path. Such a connection is therefore discarded: the pool closes it and opens a new one.
 */
class SchemaCrawlerConnectionSpec extends Specification {

    List<Connection> opened
    List<Connection> closed
    HikariDataSource pool
    SqlClientRx sqlClient

    def setup() {
        opened = new CopyOnWriteArrayList<>()
        closed = new CopyOnWriteArrayList<>()
        // the pool opens connections on its own threads, the stubs are created here
        Queue<Connection> available = new ConcurrentLinkedQueue<>((1..3).collect { newConnection() })

        DataSource driver = Stub(DataSource)
        driver.getConnection() >> {
            Connection connection = available.remove()
            opened.add(connection)
            return connection
        }

        HikariConfig config = new HikariConfig()
        config.setDataSource(driver)
        config.setMaximumPoolSize(1)
        config.setMinimumIdle(0)
        config.setInitializationFailTimeout(-1)
        pool = new HikariDataSource(config)

        sqlClient = new SqlClientRx(pool, Mock(SqlDbmsAdapter), Mock(SqlDialect), Optional.empty())
    }

    def cleanup() {
        pool.close()
    }

    def "a released connection returns to the pool"() {
        when:
        Connection first = sqlClient.getConnection()
        first.close()
        Connection second = sqlClient.getConnection()

        then:
        opened.size() == 1
        second.unwrap(Connection) == opened[0]
        closed.isEmpty()
    }

    def "a discarded connection is closed and the pool opens a new one"() {
        when:
        Connection first = sqlClient.getConnection()
        sqlClient.discardConnection(first)
        Connection second = sqlClient.getConnection()

        then:
        opened.size() == 2
        second.unwrap(Connection) == opened[1]
        new PollingConditions(timeout: 5).eventually {
            assert closed == [opened[0]]
        }
    }

    def "a discarded connection without a pool is closed"() {
        given:
        Connection connection = Mock(Connection)
        DataSource dataSource = Stub(DataSource)
        dataSource.getConnection() >> connection
        SqlClientRx plainClient =
                new SqlClientRx(dataSource, Mock(SqlDbmsAdapter), Mock(SqlDialect), Optional.empty())

        when:
        plainClient.discardConnection(plainClient.getConnection())

        then:
        1 * connection.close()
    }

    def "closing the crawler releases the connection as given"() {
        given:
        Connection connection = Mock(Connection)
        AutoCloseable release = Mock(AutoCloseable)

        when:
        new SqlSchemaCrawler(connection, release).close()

        then:
        1 * release.close()
        0 * connection.close()
    }

    private Connection newConnection() {
        Connection connection = Stub(Connection)
        connection.isValid(_) >> true
        connection.isClosed() >> { closed.contains(connection) }
        connection.close() >> { closed.add(connection) }
        return connection
    }
}
