/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import com.google.common.collect.ImmutableList
import de.ii.xtraplatform.features.domain.*
import de.ii.xtraplatform.features.sql.domain.SqlRow
import de.ii.xtraplatform.features.sql.domain.SqlRowFixtures
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

/**
 * @author zahnen
 */
@Ignore
//TODO: use SchemaSql
class FeatureDecoderSqlSpec extends Specification {

    static final Logger LOGGER = LoggerFactory.getLogger(FeatureDecoderSqlSpec.class)

    @Shared
    Reactive reactive
    @Shared
    Reactive.Runner runner

    FeatureTokenDecoder<SqlRow, FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> collectionDecoder
    FeatureTokenDecoder<SqlRow, FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> singleDecoder

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test")
    }

    def cleanupSpec() {
        runner.close()
    }

    def setup() {
        collectionDecoder = new FeatureDecoderSql(
                SqlRowFixtures.TYPE_INFOS, ImmutableList.of(),
                new ImmutableFeatureSchema.Builder().name("test").build(),
                ImmutableFeatureQuery.builder()
                        .type("biotop")
                        .build(),
                false)
        singleDecoder = new FeatureDecoderSql(
                SqlRowFixtures.TYPE_INFOS, ImmutableList.of(),
                new ImmutableFeatureSchema.Builder().name("test").build(),
                ImmutableFeatureQuery.builder()
                        .type("biotop")
                        .returnsSingleFeature(true)
                        .build(),
                false)
    }

    public <T> T runStream(Reactive.Stream<T> stream) {
        def result = stream.on(runner).run().toCompletableFuture().join()
        println(result.toString())
        return result
    }

    static Reactive.Source<SqlRow> ListSource(List<SqlRow> sqlRows) {
        Reactive.Source.iterable(sqlRows)
    }

    static Reactive.SinkReduced<Object, List<Object>> ListSink() {
        Reactive.SinkReduced.reduce([], (list2, element) -> {
            list2 << element
            return list2
        })
    }

    def 'single feature'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_TOKENS

    }

    def 'single feature with point geometry'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE_POINT)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_POINT_TOKENS

    }

    def 'single feature with multipoint geometry'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE_MULTI_POINT)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_MULTI_POINT_TOKENS

    }

    def 'single feature with multipolygon geometry'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE_MULTI_POLYGON)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_MULTI_POLYGON_TOKENS

    }

    def 'single feature with nested object'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE_NESTED_OBJECT)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_NESTED_OBJECT_TOKENS

    }

    def 'single feature with nested object arrays and value arrays'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.SINGLE_FEATURE_NESTED_OBJECT_ARRAYS)
                .via(singleDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.SINGLE_FEATURE_NESTED_OBJECT_ARRAYS_TOKENS

    }

    def 'collection with 3 features'() {

        given:

        Reactive.Stream<List<Object>> stream = ListSource(SqlRowFixtures.COLLECTION)
                .via(collectionDecoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == SqlRowFixtures.COLLECTION_TOKENS

    }
}
