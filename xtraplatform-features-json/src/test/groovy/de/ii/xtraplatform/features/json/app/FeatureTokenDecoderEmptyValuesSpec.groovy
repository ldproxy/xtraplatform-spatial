/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.app

import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.json.domain.FeatureTokenDecoderGeoJson
import de.ii.xtraplatform.geometries.domain.Axes
import de.ii.xtraplatform.streams.app.ReactiveRx
import de.ii.xtraplatform.streams.domain.Reactive
import spock.lang.Shared
import spock.lang.Specification

/**
 * The empty-value check rides the decoding of the request body, so these specs drive the real
 * decoder rather than a handler in isolation: what reaches the check is what the decoder resolved.
 */
class FeatureTokenDecoderEmptyValuesSpec extends Specification {

    @Shared
    Reactive reactive
    @Shared
    Reactive.Runner runner

    def setupSpec() {
        reactive = new ReactiveRx()
        runner = reactive.runner("test")
    }

    def cleanupSpec() {
        runner.close()
    }

    private static String feature(String properties, String id = '"B.1"') {
        return """
            {
              "type": "Feature",
              "id": ${id},
              "geometry": {"type": "Point", "coordinates": [8.7, 49.4]},
              "properties": ${properties}
            }
        """
    }

    def 'an empty string is rejected and the message names the property'() {
        when:
        run(source(feature('{"function": ""}'), true))

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
        rootCause(e).message.contains("'function' has an empty value")
    }

    def 'a string of only whitespace is rejected'() {
        when:
        run(source(feature('{"function": "  \\t "}'), true))

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
    }

    def 'an empty value in a nested object is rejected'() {
        when:
        run(source(feature('{"lifetime": {"end": ""}}'), true))

        then:
        Throwable e = thrown()
        rootCause(e).message.contains('lifetime.end')
    }

    def 'an empty value in an array is rejected'() {
        when:
        run(source(feature('{"tags": ["a", "", "c"]}'), true))

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
    }

    def 'an empty id is rejected'() {
        when:
        run(source(feature('{"function": "commercial"}', '""'), true))

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
    }

    def 'a body without empty values is decoded'() {
        when:
        List<Object> tokens = run(source(feature('{"function": "commercial", "count": 0}'), true))

        then:
        notThrown(Throwable)
        tokens.contains('commercial')
    }

    def 'null states the absence of a value and is not rejected'() {
        when:
        List<Object> tokens = run(source(feature('{"function": null}'), true))

        then: 'the value never reaches the check, so the feature is decoded'
        notThrown(Throwable)
        tokens.contains('B.1')
    }

    def 'a value that is not a string cannot be empty'() {
        when:
        List<Object> tokens = run(source(feature('{"count": 0, "flag": false}'), true))

        then:
        notThrown(Throwable)
        tokens.contains('0')
    }

    def 'an empty value is decoded as usual without the option'() {
        when:
        List<Object> tokens = run(source(feature('{"function": ""}'), false))

        then:
        notThrown(Throwable)
        tokens.contains('')
    }

    private Reactive.Stream<List<Object>> source(String body, boolean rejectEmptyValues) {
        Reactive.Source.inputStream(new ByteArrayInputStream(body.getBytes('UTF-8')))
                .via(new FeatureTokenDecoderGeoJson(
                        Optional.empty(), OgcCrs.CRS84, Axes.XY, List.of(), Set.of(),
                        rejectEmptyValues))
                .to(Reactive.Sink.reduce([], (list, element) -> {
                    list << element
                    return list
                }))
    }

    private List<Object> run(Reactive.Stream<List<Object>> stream) {
        stream.on(runner).run().toCompletableFuture().join()
    }

    private static Throwable rootCause(Throwable e) {
        Throwable cause = e
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause()
        }
        cause
    }
}
