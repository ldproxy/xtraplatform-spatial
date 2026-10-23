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

class FeatureTokenDecoderReadOnlySpec extends Specification {

    static final String FEATURE = '''
        {
          "type": "Feature",
          "id": "B.1",
          "geometry": {"type": "Point", "coordinates": [8.7, 49.4]},
          "properties": {"function": "commercial", "updated": "2001-01-01T00:00:00Z"}
        }
    '''

    static final String WITHOUT_READ_ONLY = '''
        {
          "type": "Feature",
          "id": "B.1",
          "geometry": {"type": "Point", "coordinates": [8.7, 49.4]},
          "properties": {"function": "commercial"}
        }
    '''

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

    def 'a body that sets a read-only property is rejected'() {
        given: 'a decoder for a feature type whose "updated" property is read-only'
        Reactive.Stream<List<Object>> stream = source(FEATURE, ['updated'] as Set)

        when:
        run(stream)

        then:
        Throwable e = thrown()
        rootCause(e) instanceof IllegalArgumentException
        rootCause(e).message.contains("'updated' is read-only")
    }

    def 'a body that leaves the read-only property out is decoded'() {
        given:
        Reactive.Stream<List<Object>> stream = source(WITHOUT_READ_ONLY, ['updated'] as Set)

        when:
        List<Object> tokens = run(stream)

        then: 'the feature is decoded as usual'
        notThrown(Throwable)
        tokens.contains('commercial')
    }

    def 'the same body is decoded when the feature type has no read-only property'() {
        given: 'the property is receivable, so it may be set'
        Reactive.Stream<List<Object>> stream = source(FEATURE, [] as Set)

        when:
        List<Object> tokens = run(stream)

        then:
        notThrown(Throwable)
        tokens.contains('2001-01-01T00:00:00Z')
    }

    private Reactive.Stream<List<Object>> source(String body, Set<String> readOnly) {
        Reactive.Source.inputStream(new ByteArrayInputStream(body.getBytes('UTF-8')))
                .via(new FeatureTokenDecoderGeoJson(
                        Optional.empty(), OgcCrs.CRS84, Axes.XY, List.of(), readOnly))
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
