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
import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.ConnectionInfo
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.ImmutableMultiFeatureQuery
import de.ii.xtraplatform.features.domain.ImmutableSubQuery
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.app.DecoderFactorySqlExpression
import de.ii.xtraplatform.features.sql.app.FeatureQueryEncoderSql
import de.ii.xtraplatform.features.sql.app.FilterEncoderSql
import de.ii.xtraplatform.features.sql.app.SqlMappingDeriver
import de.ii.xtraplatform.features.sql.app.SqlQueryTemplatesDeriver
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Semaphore
import java.util.regex.Matcher

/**
 * A single-shot query expression (supportPaging=false) may cap the features of each query. For a
 * feature type with more than one main table (type-level concat), the cap is shared by the main
 * tables: bounded counts, one main table after the other, determine how many features each main
 * table contributes.
 */
class SqlConnectorMaxFeaturesSpec extends Specification {

    static final String TABLE_1 = "o73005"
    static final String TABLE_2 = "o73004"

    @Shared
    Reactive.Runner runner
    @Shared
    FeatureQueryEncoderSql encoder

    def setupSpec() {
        runner = new ReactiveRx().runner("test")

        def defaults = new ImmutableSqlPathDefaults.Builder().build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
        def templatesDeriver = new SqlQueryTemplatesDeriver(filterEncoder, new SqlDialectPgis(), true, false, Optional.empty())

        def schema = FeatureSchemaFixtures.fromYaml("concat_root_objects")
                .accept(new MappingOperationResolver(), List.of())
        def rules = MappingRuleFixtures.fromYaml("concat_root_objects")
        def templates = mappingDeriver.derive(rules, schema).stream().map(templatesDeriver::derive).toList()
        assert templates.size() == 2

        encoder = new FeatureQueryEncoderSql(Map.of("concat", templates), Map.of("concat", templates),
                new ImmutableQueryGeneratorSettings.Builder().build(), new SqlDialectPgis())
    }

    def cleanupSpec() {
        runner.close()
    }

    SqlQueryBatch singleShot(int maxFeaturesPerSubQuery) {
        def query = ImmutableMultiFeatureQuery.builder()
                .addQueries(ImmutableSubQuery.builder().collectionId("c").type("concat").build())
                .supportPaging(false)
                .maxFeaturesPerSubQuery(maxFeaturesPerSubQuery)
                .build()
        return encoder.encode(query, Map.of())
    }

    RecordingSqlClient run(SqlQueryBatch batch, Map<String, Long> matching) {
        def sqlClient = new RecordingSqlClient(matching)
        new TestSqlConnector(sqlClient)
                .getSourceStream(batch, SqlQueryOptions.single())
                .to(Reactive.Sink.ignore())
                .on(runner)
                .run()
                .toCompletableFuture()
                .join()
        return sqlClient
    }

    static List<String> limits(List<String> queries) {
        return queries.collect { query ->
            Matcher matcher = query =~ /LIMIT (\d+)/
            matcher.find() ? matcher.group(1) : "none"
        }
    }

    def 'the remaining features are read from the second main table'() {

        given: 'a cap of 5, 3 matching features in the first and 10 in the second main table'
        def batch = singleShot(5)

        when:
        def sqlClient = run(batch, [(TABLE_1): 3L, (TABLE_2): 10L])

        then: 'only the first main table is counted, bounded by the cap'
        sqlClient.countQueries.size() == 1
        limits(sqlClient.countQueries(TABLE_1)) == ["5"]

        and: 'the first main table is read with its count, the second with the remainder'
        !sqlClient.valueQueries(TABLE_1).isEmpty()
        limits(sqlClient.valueQueries(TABLE_1)).every { it == "3" }
        !sqlClient.valueQueries(TABLE_2).isEmpty()
        limits(sqlClient.valueQueries(TABLE_2)).every { it == "2" }
    }

    def 'the second main table is not read when the first one reaches the cap'() {

        given: 'a cap of 5, 7 matching features in the first main table'
        def batch = singleShot(5)

        when:
        def sqlClient = run(batch, [(TABLE_1): 7L, (TABLE_2): 10L])

        then: 'the first main table is read up to the cap, the second not at all'
        limits(sqlClient.countQueries(TABLE_1)) == ["5"]
        limits(sqlClient.valueQueries(TABLE_1)).every { it == "5" }
        sqlClient.valueQueries(TABLE_2).isEmpty()
    }

    def 'without a cap all main tables are read without counts and without a limit'() {

        given:
        def batch = singleShot(0)

        when:
        def sqlClient = run(batch, [(TABLE_1): 7L, (TABLE_2): 10L])

        then:
        sqlClient.countQueries.isEmpty()
        limits(sqlClient.valueQueries(TABLE_1)).every { it == "none" }
        limits(sqlClient.valueQueries(TABLE_2)).every { it == "none" }
        !sqlClient.valueQueries(TABLE_1).isEmpty()
        !sqlClient.valueQueries(TABLE_2).isEmpty()
    }

    /** Answers count queries from the number of matching features per main table, records all queries. */
    static class RecordingSqlClient implements SqlClient {

        final Map<String, Long> matching
        final List<String> countQueries = new CopyOnWriteArrayList<>()
        final List<String> valueQueries = new CopyOnWriteArrayList<>()

        RecordingSqlClient(Map<String, Long> matching) {
            this.matching = matching
        }

        static String mainTable(String query) {
            Matcher matcher = query =~ /FROM (\w+) A/
            matcher.find()
            return matcher.group(1)
        }

        List<String> countQueries(String table) {
            return countQueries.findAll { mainTable(it) == table }
        }

        List<String> valueQueries(String table) {
            return valueQueries.findAll { mainTable(it) == table }
        }

        @Override
        Reactive.Source<SqlRow> getSourceStream(String query, SqlQueryOptions options) {
            if (query.contains("numberReturned")) {
                countQueries.add(query)
                Matcher limit = query =~ /LIMIT (\d+)/
                long bound = limit.find() ? Long.parseLong(limit.group(1)) : Long.MAX_VALUE
                long numberReturned = Math.min(matching.get(mainTable(query)), bound)
                return Reactive.Source.single(new MetaRow([null, null, numberReturned, -1L, -1L]))
            }
            valueQueries.add(query)
            return Reactive.Source.empty()
        }

        @Override
        CompletableFuture<Collection<SqlRow>> run(String query, SqlQueryOptions options) {
            throw new UnsupportedOperationException()
        }

        @Override
        List<String> getNotifications(Connection connection) {
            return List.of()
        }

        @Override
        Connection getConnection() {
            throw new UnsupportedOperationException()
        }

        @Override
        SqlDialect getSqlDialect() {
            return new SqlDialectPgis()
        }

        @Override
        SqlDbmsAdapter getDbmsAdapter() {
            throw new UnsupportedOperationException()
        }
    }

    static class MetaRow implements SqlRow {

        final List<Object> values

        MetaRow(List<Object> values) {
            this.values = values
        }

        @Override
        List<Object> getValues() {
            return values
        }

        @Override
        Optional<String> getType() {
            return Optional.empty()
        }
    }

    static class TestSqlConnector implements SqlConnector {

        final SqlClient sqlClient
        final Semaphore connectionBudget = new Semaphore(10, true)

        TestSqlConnector(SqlClient sqlClient) {
            this.sqlClient = sqlClient
        }

        @Override
        SqlClient getSqlClient() {
            return sqlClient
        }

        @Override
        Semaphore getConnectionBudget() {
            return connectionBudget
        }

        @Override
        int getMaxConnections() {
            return 10
        }

        @Override
        int getMinConnections() {
            return 1
        }

        @Override
        int getQueueSize() {
            return 0
        }

        @Override
        String getDialect() {
            return "PGIS"
        }

        @Override
        String getType() {
            return "SQL"
        }

        @Override
        String getProviderId() {
            return "test"
        }

        @Override
        void start() {}

        @Override
        void stop() {}

        @Override
        boolean isConnected() {
            return true
        }

        @Override
        Optional<Throwable> getConnectionError() {
            return Optional.empty()
        }

        @Override
        boolean isSameDataset(ConnectionInfo connectionInfo) {
            return false
        }

        @Override
        String getDatasetIdentifier() {
            return "test"
        }

        @Override
        Volatile2.State getState() {
            return Volatile2.State.AVAILABLE
        }

        @Override
        Optional<String> getMessage() {
            return Optional.empty()
        }

        @Override
        Runnable onStateChange(VolatileRegistry.ChangeHandler handler, boolean initialCall) {
            return () -> {}
        }
    }
}
