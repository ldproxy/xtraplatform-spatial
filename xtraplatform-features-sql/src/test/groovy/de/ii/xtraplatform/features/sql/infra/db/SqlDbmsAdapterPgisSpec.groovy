/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import de.ii.xtraplatform.base.domain.AppContext
import de.ii.xtraplatform.features.sql.domain.ImmutableConnectionInfoSql
import org.postgresql.ds.PGSimpleDataSource
import spock.lang.Specification

class SqlDbmsAdapterPgisSpec extends Specification {

    SqlDbmsAdapterPgis adapter

    def setup() {
        def appContext = Stub(AppContext)
        appContext.getName() >> "test"
        appContext.getVersion() >> "1.0"
        adapter = new SqlDbmsAdapterPgis(appContext)
    }

    def connectionInfo(Map<String, String> driverOptions) {
        return new ImmutableConnectionInfoSql.Builder()
                .host("localhost")
                .database("db")
                .user("user")
                .driverOptions(driverOptions)
                .build()
    }

    def 'socketTimeout, connectTimeout and tcpKeepAlive are passed to the driver'() {
        when:
        def ds = (PGSimpleDataSource) adapter.createDataSource("test",
                connectionInfo([socketTimeout: "45", connectTimeout: "10", tcpKeepAlive: "true"]))

        then:
        ds.getSocketTimeout() == 45
        ds.getConnectTimeout() == 10
        ds.getTcpKeepAlive()
    }

    def 'driver defaults are kept when the options are not set'() {
        when:
        def ds = (PGSimpleDataSource) adapter.createDataSource("test", connectionInfo([:]))

        then:
        ds.getSocketTimeout() == 0
        ds.getConnectTimeout() == new PGSimpleDataSource().getConnectTimeout()
        !ds.getTcpKeepAlive()
    }

    def 'an invalid timeout value fails the provider startup with a clear message'() {
        when:
        adapter.createDataSource("test", connectionInfo([socketTimeout: value]))

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains("socketTimeout")

        where:
        value << ["abc", "-1", "1.5"]
    }
}
