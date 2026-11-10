/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain

import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.FeatureTransactions
import de.ii.xtraplatform.features.domain.ImmutableMutationResult
import de.ii.xtraplatform.features.domain.SchemaBase.Role
import de.ii.xtraplatform.features.domain.Tuple
import spock.lang.Specification

import java.time.Instant

class FeatureTokenStatsCollectorSpec extends Specification {

    static final String JAN = "2024-01-01T00:00:00Z"
    static final String MAR = "2024-03-01T00:00:00Z"
    static final String APR = "2024-04-01T00:00:00Z"

    def "a feature without an end makes the end unbounded, whatever the other features have"() {

        given: 'two features written in one mutation, from separate sources'
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, false, JAN, null)
        interval(stats, false, MAR, APR)

        expect:
        temporal(stats) == Tuple.of(millis(JAN), null)
    }

    def "a feature without a start makes the start unbounded"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, false, MAR, APR)
        interval(stats, false, null, MAR)

        expect:
        temporal(stats) == Tuple.of(null, millis(APR))
    }

    def "features without instants are ignored"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        stats.addInstant(MAR)
        stats.addInstant(JAN)

        expect:
        temporal(stats) == Tuple.of(millis(JAN), millis(MAR))
    }

    def "no temporal extent without temporal values"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)

        expect:
        temporal(stats) == null
    }

    def "a type with only a start is unbounded at the end"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, false, JAN, null, true, false)

        expect:
        temporal(stats) == Tuple.of(millis(JAN), null)
    }

    def "a type with only an end is unbounded at the start"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, false, null, APR, false, true)

        expect:
        temporal(stats) == Tuple.of(null, millis(APR))
    }

    def "the values set by the mutation replace those of the features"() {

        given: 'a new version: the mutation sets the start and leaves the end open'
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, false, null, null)
        def overrides = new HashMap<Role, Object>()
        overrides.put(Role.PRIMARY_INTERVAL_START, MAR)
        overrides.put(Role.PRIMARY_INTERVAL_END, null)
        stats.applyRoleOverrides(overrides)

        expect:
        temporal(stats) == Tuple.of(millis(MAR), null)
    }

    def "a partial update without start or end leaves the extent as it is"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, true, null, null)

        expect:
        temporal(stats) == null
    }

    def "a partial update with an end only uses it for both sides"() {

        given: 'the start is unchanged and not after the end'
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, true, null, APR)

        expect:
        temporal(stats) == Tuple.of(millis(APR), millis(APR))
    }

    def "a partial update that clears the end makes it unbounded"() {

        given:
        def stats = new FeatureTokenStatsCollector.Stats(OgcCrs.CRS84)
        interval(stats, true, MAR, FeatureTransactions.PATCH_NULL_VALUE)

        expect:
        temporal(stats) == Tuple.of(millis(MAR), null)
    }

    // one feature as the collector passes it on at the end of the feature
    static void interval(FeatureTokenStatsCollector.Stats stats, boolean partial, String start, String end,
                         boolean hasStart = true, boolean hasEnd = true) {
        def collector = new FeatureTokenStatsCollector(stats, partial)
        collector.featureStart = start
        collector.featureEnd = end
        collector.addInterval(mapping(hasStart, hasEnd))
    }

    static SqlQueryMapping mapping(boolean hasStart, boolean hasEnd) {
        def schema = new de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder()
                .name("t").type(de.ii.xtraplatform.features.domain.SchemaBase.Type.DATETIME).build()
        return [getSchemaForRole: { Role role ->
            (role == Role.PRIMARY_INTERVAL_START && hasStart) || (role == Role.PRIMARY_INTERVAL_END && hasEnd)
                    ? Optional.of(schema) : Optional.empty()
        }] as SqlQueryMapping
    }

    static Tuple<Long, Long> temporal(FeatureTokenStatsCollector.Stats stats) {
        def builder = ImmutableMutationResult.builder().type(FeatureTransactions.MutationResult.Type.CREATE).hasFeatures(false)
        stats.writeTo(builder)
        builder.build().getTemporalExtent().orElse(null)
    }

    static Long millis(String instant) {
        Instant.parse(instant).toEpochMilli()
    }
}
