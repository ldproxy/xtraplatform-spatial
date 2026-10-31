/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class FeatureTypeExtentSpec extends Specification {

    static SpatialExtent FIXED_SPATIAL = new ImmutableSpatialExtent.Builder().xmin(0).ymin(0).xmax(1).ymax(1).build()
    static SpatialExtent OTHER_SPATIAL = new ImmutableSpatialExtent.Builder().xmin(5).ymin(5).xmax(6).ymax(6).build()
    static SpatialExtent COMPUTED_SPATIAL = new ImmutableSpatialExtent.Builder().computed(true).build()
    static TemporalExtent FIXED_TEMPORAL = new ImmutableTemporalExtent.Builder().start("2020-01-01").build()

    static Optional<FeatureTypeExtent> extent(SpatialExtent spatial, TemporalExtent temporal) {
        Optional.of(ImmutableFeatureTypeExtent.builder()
                .spatial(Optional.ofNullable(spatial))
                .temporal(Optional.ofNullable(temporal))
                .build())
    }

    def "a type without extent inherits the provider extent"() {
        when:
        def resolved = FeatureTypeExtent.inherit(Optional.empty(), extent(FIXED_SPATIAL, FIXED_TEMPORAL))

        then:
        resolved.spatial == Optional.of(FIXED_SPATIAL)
        resolved.temporal == Optional.of(FIXED_TEMPORAL)
    }

    def "the type extent replaces the provider extent per part"() {
        when:
        def resolved = FeatureTypeExtent.inherit(extent(OTHER_SPATIAL, null), extent(FIXED_SPATIAL, FIXED_TEMPORAL))

        then:
        resolved.spatial == Optional.of(OTHER_SPATIAL)
        resolved.temporal == Optional.of(FIXED_TEMPORAL)
    }

    def "computed on the type discards the inherited fixed extent"() {
        when:
        def resolved = FeatureTypeExtent.inherit(extent(COMPUTED_SPATIAL, null), extent(FIXED_SPATIAL, null))

        then:
        resolved.spatial.flatMap { it.toBoundingBox(de.ii.xtraplatform.crs.domain.OgcCrs.CRS84) }.isEmpty()
    }

    def "nothing configured means nothing fixed"() {
        when:
        def resolved = FeatureTypeExtent.inherit(Optional.empty(), Optional.empty())

        then:
        resolved.spatial.isEmpty()
        resolved.temporal.isEmpty()
    }
}
