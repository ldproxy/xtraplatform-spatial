/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform

import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableCrsVariants
import de.ii.xtraplatform.features.domain.SchemaBase
import spock.lang.Specification

/**
 * {@link CrsVariantsResolver}: properties referenced from a {@code crsVariants} declaration are marked
 * {@code internal} and receive their implied role; invalid declarations (missing sibling, wrong
 * type, geometry variant without a storage CRS or without identifiers) are rejected at provider
 * start.
 */
class CrsVariantsResolverSpec extends Specification {

    def resolver = new CrsVariantsResolver()

    static ImmutableFeatureSchema.Builder type(Map<String, ImmutableFeatureSchema.Builder> props) {
        def builder = new ImmutableFeatureSchema.Builder()
                .name("ax_punktortau")
                .sourcePath("/o14003")
                .type(SchemaBase.Type.OBJECT)
        props.each { name, prop -> builder.putProperties2(name, prop) }
        builder
    }

    static ImmutableFeatureSchema.Builder geometryWithVariants() {
        new ImmutableFeatureSchema.Builder()
                .sourcePath("position")
                .type(SchemaBase.Type.GEOMETRY)
                .role(SchemaBase.Role.PRIMARY_GEOMETRY)
                .crsVariants(new ImmutableCrsVariants.Builder()
                        .crsProperty("pos_srs")
                        .verticalProperty("pos_h")
                        .addGeometryProperties("pos_gk3")
                        .build())
    }

    static Map<String, ImmutableFeatureSchema.Builder> validSiblings() {
        [
                "pos_srs": new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_srs")
                        .type(SchemaBase.Type.STRING),
                "pos_gk3": new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_gk3")
                        .type(SchemaBase.Type.GEOMETRY)
                        .nativeCrs(EpsgCrs.of(5677))
                        .addOriginalCrsIdentifiers("urn:adv:crs:DE_DHDN_3GK3_HE100")
                        .falseEastingDifference(3000000d),
                "pos_h"  : new ImmutableFeatureSchema.Builder()
                        .sourcePath("position_h")
                        .type(SchemaBase.Type.FLOAT)
                        .addOriginalCrsIdentifiers("urn:adv:crs:DE_DHHN92_NH"),
        ]
    }

    def 'referenced siblings are marked internal, other properties are untouched'() {
        given:
        def props = validSiblings()
        props["other"] = new ImmutableFeatureSchema.Builder()
                .sourcePath("other")
                .type(SchemaBase.Type.STRING)
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        expect:
        resolver.needsResolving(types)

        when:
        def resolved = resolver.resolve(types)
        def resolvedType = resolved["ax_punktortau"]

        then:
        resolvedType.getPropertyMap()["pos_srs"].isInternal()
        resolvedType.getPropertyMap()["pos_gk3"].isInternal()
        resolvedType.getPropertyMap()["pos_h"].isInternal()
        !resolvedType.getPropertyMap()["other"].isInternal()
        !resolvedType.getPropertyMap()["position"].isInternal()

        and: 'the group members receive their implied role'
        resolvedType.getPropertyMap()["pos_srs"].getRole() == Optional.of(SchemaBase.Role.ORIGINAL_CRS_IDENTIFIER)
        resolvedType.getPropertyMap()["pos_gk3"].getRole() == Optional.of(SchemaBase.Role.ORIGINAL_GEOMETRY)
        resolvedType.getPropertyMap()["pos_h"].getRole() == Optional.of(SchemaBase.Role.ORIGINAL_HEIGHT)

        and: 'the resolved types need no further resolution'
        !resolver.needsResolving(resolved)
    }

    def 'internal properties are neither queryable nor sortable'() {
        given:
        def props = validSiblings()
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        def resolvedType = resolver.resolve(types)["ax_punktortau"]

        then:
        !resolvedType.getPropertyMap()["pos_srs"].queryable()
        !resolvedType.getPropertyMap()["pos_srs"].sortable()
        !resolvedType.getPropertyMap()["pos_h"].queryable()
        !resolvedType.getPropertyMap()["pos_h"].sortable()
    }

    def 'a crsVariants declaration referencing a missing sibling is rejected'() {
        given:
        def props = validSiblings()
        props.remove("pos_gk3")
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        resolver.resolve(types)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("pos_gk3")
    }

    def 'a geometry variant without a storage CRS is rejected'() {
        given:
        def props = validSiblings()
        props["pos_gk3"] = new ImmutableFeatureSchema.Builder()
                .sourcePath("position_gk3")
                .type(SchemaBase.Type.GEOMETRY)
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        resolver.resolve(types)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("nativeCrs")
    }

    def 'a geometry variant without originalCrsIdentifiers is rejected'() {
        given:
        def props = validSiblings()
        props["pos_gk3"] = new ImmutableFeatureSchema.Builder()
                .sourcePath("position_gk3")
                .type(SchemaBase.Type.GEOMETRY)
                .nativeCrs(EpsgCrs.of(5677))
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        resolver.resolve(types)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("originalCrsIdentifiers")
    }

    def 'a conflicting explicit role on a group member is rejected'() {
        given:
        def props = validSiblings()
        props["pos_h"] = new ImmutableFeatureSchema.Builder()
                .sourcePath("position_h")
                .type(SchemaBase.Type.FLOAT)
                .role(SchemaBase.Role.ORIGINAL_GEOMETRY)
                .addOriginalCrsIdentifiers("urn:adv:crs:DE_DHHN92_NH")
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        resolver.resolve(types)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("ORIGINAL_HEIGHT")
    }

    def 'a crsProperty that is not a STRING is rejected'() {
        given:
        def props = validSiblings()
        props["pos_srs"] = new ImmutableFeatureSchema.Builder()
                .sourcePath("position_srs")
                .type(SchemaBase.Type.INTEGER)
        props["position"] = geometryWithVariants()
        def types = ["ax_punktortau": type(props).build() as FeatureSchema]

        when:
        resolver.resolve(types)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains("STRING")
    }
}
