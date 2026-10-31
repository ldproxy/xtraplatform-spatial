/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.cql.domain.Bbox
import de.ii.xtraplatform.cql.domain.Interval
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.SIntersects
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.cql.domain.TIntersects
import de.ii.xtraplatform.cql.domain.TemporalLiteral
import de.ii.xtraplatform.crs.domain.BoundingBox
import de.ii.xtraplatform.crs.domain.EpsgCrs
import spock.lang.Specification

import java.time.Instant

class ExtentFiltersSpec extends Specification {

    static BoundingBox BBOX = BoundingBox.of(300000, 5600000, 400000, 5700000, EpsgCrs.of(25832))
    static org.threeten.extra.Interval INTERVAL =
            org.threeten.extra.Interval.of(Instant.parse("2020-01-01T00:00:00Z"), Instant.MAX)

    static FeatureSchema type(Map<String, SchemaBase.Role> temporalRoles) {
        def builder = new ImmutableFeatureSchema.Builder()
                .name("t").sourcePath("/t").type(SchemaBase.Type.OBJECT)
                .putProperties2("geom", new ImmutableFeatureSchema.Builder()
                        .sourcePath("geom").type(SchemaBase.Type.GEOMETRY)
                        .role(SchemaBase.Role.PRIMARY_GEOMETRY))
        temporalRoles.each { name, role ->
            builder.putProperties2(name, new ImmutableFeatureSchema.Builder()
                    .sourcePath(name).type(SchemaBase.Type.DATETIME).role(role))
        }
        builder.build()
    }

    def "spatial and instant extents become intersects filters"() {
        when:
        def filters = ExtentFilters.of(type(["date": SchemaBase.Role.PRIMARY_INSTANT]), Optional.of(BBOX), Optional.of(INTERVAL))

        then:
        filters == [
                SIntersects.of(Property.of("geom"), SpatialLiteral.of(Bbox.of(BBOX))),
                TIntersects.of(Property.of("date"), new TemporalLiteral.Builder(INTERVAL).build())
        ]
    }

    def "an interval extent uses both interval properties"() {
        when:
        def filters = ExtentFilters.of(
                type(["begin": SchemaBase.Role.PRIMARY_INTERVAL_START, "end": SchemaBase.Role.PRIMARY_INTERVAL_END]),
                Optional.empty(), Optional.of(INTERVAL))

        then:
        filters == [
                TIntersects.of(
                        Interval.of([Property.of("begin"), Property.of("end")]),
                        new TemporalLiteral.Builder(INTERVAL).build())
        ]
    }

    def "no extent or no matching property means no filter"() {
        expect:
        ExtentFilters.of(type([:]), Optional.empty(), Optional.empty()).isEmpty()
        ExtentFilters.of(type([:]), Optional.empty(), Optional.of(INTERVAL)).isEmpty()
    }
}
