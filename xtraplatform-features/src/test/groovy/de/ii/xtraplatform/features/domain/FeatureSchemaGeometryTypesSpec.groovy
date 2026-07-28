/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.geometries.domain.GeometryType
import spock.lang.Specification

class FeatureSchemaGeometryTypesSpec extends Specification {

    static FeatureSchema geom(GeometryType single, List<GeometryType> multi) {
        def b = new ImmutableFeatureSchema.Builder()
                .name("g")
                .type(SchemaBase.Type.GEOMETRY)
                .sourcePath("g")
        if (single != null) {
            b.geometryType(single)
        }
        if (multi != null) {
            b.geometryTypes(multi)
        }
        return b.build()
    }

    def "property with no geometry types: effective is ANY, list contains ANY"() {
        when:
        def schema = geom(null, null)

        then:
        schema.effectiveGeometryType == GeometryType.ANY
        schema.effectiveGeometryTypes == [GeometryType.ANY]
    }

    def "property with only geometryType: effective is that type"() {
        when:
        def schema = geom(GeometryType.POINT, null)

        then:
        schema.effectiveGeometryType == GeometryType.POINT
        schema.effectiveGeometryTypes == [GeometryType.POINT]
    }

    def "property with single entry in geometryTypes: effective is that entry"() {
        when:
        def schema = geom(null, [GeometryType.MULTI_POLYGON])

        then:
        schema.effectiveGeometryType == GeometryType.MULTI_POLYGON
        schema.effectiveGeometryTypes == [GeometryType.MULTI_POLYGON]
    }

    def "property with two simple-feature entries: effective is ANY"() {
        when:
        def schema = geom(null, [GeometryType.POINT, GeometryType.MULTI_POINT])

        then:
        schema.effectiveGeometryType == GeometryType.ANY
        schema.effectiveGeometryTypes.toSet() ==
                [GeometryType.POINT, GeometryType.MULTI_POINT].toSet()
    }

    def "property with non-simple-feature entry: effective is ANY_EXTENDED"() {
        when:
        def schema = geom(null, [GeometryType.LINE_STRING, GeometryType.CIRCULAR_STRING])

        then:
        schema.effectiveGeometryType == GeometryType.ANY_EXTENDED
    }

    def "property with three curve entries: effective is ANY_EXTENDED"() {
        when:
        def schema = geom(null, [
                GeometryType.LINE_STRING,
                GeometryType.CIRCULAR_STRING,
                GeometryType.COMPOUND_CURVE
        ])

        then:
        schema.effectiveGeometryType == GeometryType.ANY_EXTENDED
    }

    def "property with both fields set consistently: result is that single type"() {
        when:
        def schema = geom(GeometryType.POINT, [GeometryType.POINT])

        then:
        schema.effectiveGeometryType == GeometryType.POINT
    }

    def "property with both fields set differently: geometryTypes wins"() {
        when:
        def schema = geom(GeometryType.POINT, [GeometryType.POINT, GeometryType.MULTI_POINT])

        then:
        schema.effectiveGeometryType == GeometryType.ANY
    }

    /** Builds a concat'd feature whose branches each carry one PRIMARY_GEOMETRY of the given type. */
    static FeatureSchema concatFeature(GeometryType... branchTypes) {
        def branches = branchTypes.toList().withIndex().collect { GeometryType t, int i ->
            new ImmutableFeatureSchema.Builder()
                    .name("branch${i}" as String)
                    .sourcePath("/branch${i}" as String)
                    .putProperties2("id", new ImmutableFeatureSchema.Builder()
                            .sourcePath("id")
                            .type(SchemaBase.Type.INTEGER)
                            .role(SchemaBase.Role.ID))
                    .putProperties2("geometry", new ImmutableFeatureSchema.Builder()
                            .sourcePath("geom")
                            .type(SchemaBase.Type.GEOMETRY)
                            .geometryType(t)
                            .role(SchemaBase.Role.PRIMARY_GEOMETRY))
                    .build()
        }
        return new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT_ARRAY)
                .concat(branches)
                .build()
    }

    def "non-concat feature: getPrimaryGeometries() has 0 or 1 entry"() {
        given:
        def withGeom = new ImmutableFeatureSchema.Builder()
                .name("test").type(SchemaBase.Type.OBJECT).sourcePath("/t")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id").type(SchemaBase.Type.INTEGER).role(SchemaBase.Role.ID))
                .putProperties2("geometry", new ImmutableFeatureSchema.Builder()
                        .sourcePath("geom").type(SchemaBase.Type.GEOMETRY)
                        .geometryType(GeometryType.POINT).role(SchemaBase.Role.PRIMARY_GEOMETRY))
                .build()
        def withoutGeom = new ImmutableFeatureSchema.Builder()
                .name("test").type(SchemaBase.Type.OBJECT).sourcePath("/t")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id").type(SchemaBase.Type.INTEGER).role(SchemaBase.Role.ID))
                .build()

        expect:
        withGeom.primaryGeometries.size() == 1
        withGeom.collectEffectiveGeometryTypes() == [GeometryType.POINT]
        withGeom.effectiveGeometryType == GeometryType.POINT
        withoutGeom.primaryGeometries.isEmpty()
        withoutGeom.collectEffectiveGeometryTypes().isEmpty()
        withoutGeom.effectiveGeometryType == GeometryType.ANY
    }

    def "concat with two branches, same primary geometry type: list has 2 entries, effective is that type"() {
        when:
        def schema = concatFeature(GeometryType.MULTI_POLYGON, GeometryType.MULTI_POLYGON)

        then:
        schema.primaryGeometries.size() == 2
        schema.collectEffectiveGeometryTypes() == [GeometryType.MULTI_POLYGON]
        schema.effectiveGeometryType == GeometryType.MULTI_POLYGON
    }

    def "concat with branches POINT + MULTI_POINT: list has 2 entries, effective is ANY"() {
        when:
        def schema = concatFeature(GeometryType.POINT, GeometryType.MULTI_POINT)

        then:
        schema.primaryGeometries.size() == 2
        schema.collectEffectiveGeometryTypes().toSet() ==
                [GeometryType.POINT, GeometryType.MULTI_POINT].toSet()
        schema.effectiveGeometryType == GeometryType.ANY
    }

    def "concat with three differing simple-feature branches: effective is ANY"() {
        when:
        def schema = concatFeature(
                GeometryType.POINT, GeometryType.LINE_STRING, GeometryType.POLYGON)

        then:
        schema.primaryGeometries.size() == 3
        schema.collectEffectiveGeometryTypes().toSet() ==
                [GeometryType.POINT, GeometryType.LINE_STRING, GeometryType.POLYGON].toSet()
        schema.effectiveGeometryType == GeometryType.ANY
    }
}
