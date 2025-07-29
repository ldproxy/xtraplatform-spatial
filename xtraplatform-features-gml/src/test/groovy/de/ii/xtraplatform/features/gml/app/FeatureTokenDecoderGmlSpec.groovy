/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app

import de.ii.xtraplatform.features.domain.*
import de.ii.xtraplatform.geometries.domain.GeometryType
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification

import javax.xml.namespace.QName

/**
 * @author zahnen
 */
@Ignore
class FeatureTokenDecoderGmlSpec extends Specification {

    static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenDecoderGmlSpec.class)

    @Shared
    Reactive reactive
    @Shared
    Reactive.Runner runner

    FeatureTokenDecoder<byte[], FeatureSchema, SchemaMapping, FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping>> decoder

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test")
    }

    def cleanupSpec() {
        runner.close()
    }

    def setup() {
        def schema = new ImmutableFeatureSchema.Builder()
                .name("pand")
                .sourcePath("/bag:pand")
                .type(SchemaBase.Type.OBJECT)
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("gml@id")
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.ID))
                .putProperties2("geometry", new ImmutableFeatureSchema.Builder()
                        .sourcePath("bag:geom")
                        .type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POLYGON))
                .build()

        decoder = new FeatureTokenDecoderGml(
                ["bag": "http://bag.geonovum.nl", "gml": "http://www.opengis.net/gml/3.2"],
                [new QName("http://bag.geonovum.nl", "pand")],
                schema,
                ImmutableFeatureQuery.builder().type("pand").build(),
                Map.of(
                    schema.getName(),
                    new ImmutableSchemaMapping.Builder()
                            .targetSchema(schema)
                            .sourcePathTransformer((path, isValue) -> path)
                            .build()),
                false)
    }

    public <T> T runStream(Reactive.Stream<T> stream) {
        def result = stream.on(runner).run().toCompletableFuture().join()
        println(result.toString())
        return result
    }

    static Reactive.Source<byte[]> FileSource(String path) {
        Reactive.Source.inputStream(new File(path).newInputStream())
    }

    static Reactive.SinkReduced<Object, List<Object>> ListSink() {
        Reactive.Sink.reduce([], (list2, element) -> {
            list2 << element
            return list2
        })
    }

    def 'single feature with polygon geometry with inner ring'() {

        given:

        Reactive.Stream<List<Object>> stream = FileSource('src/test/resources/simple_polygon_inner.xml')
                .via(decoder)
                .to(ListSink())

        when:

        List<Object> tokens = runStream(stream)

        then:
        tokens == FeatureTokenFixtures.SINGLE_FEATURE_POLYGON_INNER

    }

}
