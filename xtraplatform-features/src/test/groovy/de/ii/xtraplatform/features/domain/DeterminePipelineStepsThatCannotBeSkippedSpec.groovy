/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.features.domain.FeatureStream.PipelineSteps
import de.ii.xtraplatform.geometries.domain.GeometryType
import spock.lang.Specification

class DeterminePipelineStepsThatCannotBeSkippedSpec extends Specification {

    static final EpsgCrs NATIVE_CRS = EpsgCrs.of(25832)

    static FeatureSchema featureType(ImmutableFeatureSchema.Builder geometry) {
        return new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .putPropertyMap("position", geometry.build())
                .build()
    }

    static ImmutableFeatureSchema.Builder geometry() {
        return new ImmutableFeatureSchema.Builder()
                .name("position")
                .type(SchemaBase.Type.GEOMETRY)
                .geometryType(GeometryType.POINT)
                .sourcePath("position")
    }

    static Set<PipelineSteps> keepSteps(FeatureSchema schema, EpsgCrs targetCrs) {
        FeatureQuery query = ImmutableFeatureQuery.builder().type("test").build()

        return schema.accept(new DeterminePipelineStepsThatCannotBeSkipped(
                query,
                "test",
                Optional.empty(),
                NATIVE_CRS,
                targetCrs,
                false,
                false,
                false,
                true,
                false))
    }

    def "no coordinate processing when the target CRS is the native CRS"() {
        given:
        def schema = featureType(geometry())

        when:
        def steps = keepSteps(schema, NATIVE_CRS)

        then:
        !steps.contains(PipelineSteps.COORDINATES)
    }

    def "coordinate processing when the target CRS differs from the native CRS"() {
        given:
        def schema = featureType(geometry())

        when:
        def steps = keepSteps(schema, EpsgCrs.of(4326))

        then:
        steps.contains(PipelineSteps.COORDINATES)
    }

    def "coordinate processing for a position variant that restores its original axis order, even when the target CRS is the native CRS"() {
        given: "a geometry stored in its own CRS in GIS axis order with a differing original CRS (authority axis order)"
        def schema = featureType(geometry()
                .nativeCrs(EpsgCrs.of(4937, EpsgCrs.Force.LON_LAT))
                .originalCrs(EpsgCrs.of(4937)))

        when:
        def steps = keepSteps(schema, NATIVE_CRS)

        then:
        steps.contains(PipelineSteps.COORDINATES)
    }

    def "no coordinate processing for a position variant whose original CRS equals its storage CRS when the target CRS is the native CRS"() {
        given:
        def schema = featureType(geometry()
                .nativeCrs(EpsgCrs.of(5677))
                .originalCrs(EpsgCrs.of(5677)))

        when:
        def steps = keepSteps(schema, NATIVE_CRS)

        then:
        !steps.contains(PipelineSteps.COORDINATES)
    }
}
