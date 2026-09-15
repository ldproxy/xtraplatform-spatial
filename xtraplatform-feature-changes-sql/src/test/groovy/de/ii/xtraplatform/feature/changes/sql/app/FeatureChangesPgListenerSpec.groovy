/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.app

import spock.lang.Specification

import java.sql.Connection
import java.sql.SQLException
import java.util.function.Function
import java.util.function.Supplier

class FeatureChangesPgListenerSpec extends Specification {

    def 'path options are not part of a table or column name'() {
        expect:
        FeatureChangesPgListener.column(sourcePath) == name

        where:
        sourcePath              || name
        "id"                    || "id"
        "id{generated=false}"   || "id"
        "geom{force=LON_LAT}"   || "geom"
    }

    def subscription(Supplier<Connection> connections) {
        return new ImmutableSubscription.Builder()
                .index(1)
                .type("feature")
                .table("feature")
                .idColumn("id")
                .connectionFactory(connections)
                .notificationPoller(Stub(Function))
                .build()
    }

    def 'a reconnect returns the previous connection to the pool'() {
        given:
        def first = Mock(Connection)
        def second = Mock(Connection)
        def connections = [first, second].iterator()
        def connected = subscription({ connections.next() } as Supplier<Connection>).connect()

        when:
        def reconnected = connected.connect()

        then: 'the connection that is replaced is closed, so the pool can reuse it'
        1 * first.close()
        reconnected.getConnection() == second
    }

    def 'a failure while closing the previous connection does not prevent the reconnect'() {
        given:
        def first = Mock(Connection)
        def second = Mock(Connection)
        def connections = [first, second].iterator()
        def connected = subscription({ connections.next() } as Supplier<Connection>).connect()

        when:
        def reconnected = connected.connect()

        then:
        1 * first.close() >> { throw new SQLException("connection was already terminated") }
        reconnected.getConnection() == second
    }

    def 'the first connect has no previous connection to close'() {
        given:
        def connection = Mock(Connection)

        when:
        def connected = subscription({ connection } as Supplier<Connection>).connect()

        then:
        0 * connection.close()
        connected.getConnection() == connection
    }
}
