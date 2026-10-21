/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import de.ii.xtraplatform.base.domain.resiliency.Volatile2
import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.features.domain.ConnectionInfo
import de.ii.xtraplatform.features.domain.SortKey
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import org.apache.commons.lang3.function.TriFunction
import spock.lang.Shared
import spock.lang.Specification

import java.util.concurrent.Semaphore
import java.util.function.BiFunction
import java.util.stream.Stream

class SqlConnectorSourceStreamSpec extends Specification {

    @Shared
    Reactive.Runner runner

    def setupSpec() {
        runner = new ReactiveRx().runner("test")
    }

    def cleanupSpec() {
        runner.close()
    }

    def "the statements of each sub-query of a single-shot query get the sort keys of that sub-query"() {

        given: "a first sub-query sorted by identificatie and a second one sorted by oppervlakte, descending"
        List<SqlQueryOptions> received = []
        SqlConnector connector = new TestConnector(received)
        SqlQueryBatch batch = new ImmutableSqlQueryBatch.Builder()
                .limit(0)
                .offset(0)
                .chunkSize(100)
                .isUnpaged(true)
                .addQuerySets(
                        querySet(0, "pand", SortKey.of("identificatie")),
                        querySet(1, "verblijfsobject", SortKey.of("oppervlakte", SortKey.Direction.DESCENDING)))
                .build()

        when:
        connector.getSourceStream(batch, options(SortKey.of("identificatie")))
                .to(Reactive.Sink.ignore())
                .on(runner)
                .run()
                .toCompletableFuture()
                .join()

        then:
        received.collectEntries { [(it.type.get()): it.customSortKeys] } == [
                pand           : [SortKey.of("identificatie")],
                verblijfsobject: [SortKey.of("oppervlakte", SortKey.Direction.DESCENDING)]]
    }

    static SqlQuerySet querySet(int index, String table, SortKey sortKey) {
        return new ImmutableSqlQuerySet.Builder()
                .metaQuery({ limit, offset -> Optional.empty() } as BiFunction<Long, Long, Optional<String>>)
                .valueQueries({ meta, maxLimit, skipped -> Stream.of("SELECT FROM " + table) }
                        as TriFunction<SqlRowMeta, Long, Long, Stream<String>>)
                .addTableSchemas(new ImmutableSqlQuerySchema.Builder().name(table).pathSegment(table).build())
                .options(new ImmutableSqlQueryOptions.Builder().from(options(sortKey)).type(table).build())
                .queryIndex(index)
                .build()
    }

    static SqlQueryOptions options(SortKey sortKey) {
        return new ImmutableSqlQueryOptions.Builder().addCustomSortKeys(sortKey).build()
    }

    static class TestConnector implements SqlConnector {

        final List<SqlQueryOptions> received

        TestConnector(List<SqlQueryOptions> received) {
            this.received = received
        }

        @Override
        SqlClient getSqlClient() {
            return [getSourceStream: { String query, SqlQueryOptions options ->
                received << options
                return Reactive.Source.empty()
            }] as SqlClient
        }

        @Override
        int getMaxConnections() { return 4 }

        @Override
        Semaphore getConnectionBudget() { return new Semaphore(4) }

        @Override
        int getMinConnections() { return 1 }

        @Override
        int getQueueSize() { return 1 }

        @Override
        String getDialect() { return "PGIS" }

        @Override
        String getType() { return "SQL" }

        @Override
        String getProviderId() { return "test" }

        @Override
        void start() {}

        @Override
        void stop() {}

        @Override
        boolean isConnected() { return true }

        @Override
        Optional<Throwable> getConnectionError() { return Optional.empty() }

        @Override
        boolean isSameDataset(ConnectionInfo connectionInfo) { return false }

        @Override
        String getDatasetIdentifier() { return "test" }

        @Override
        Volatile2.State getState() { return Volatile2.State.AVAILABLE }

        @Override
        Optional<String> getMessage() { return Optional.empty() }

        @Override
        Runnable onStateChange(VolatileRegistry.ChangeHandler handler, boolean initialCall) { return {} }
    }
}
