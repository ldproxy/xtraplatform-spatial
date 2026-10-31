/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import org.threeten.extra.Interval
import spock.lang.Specification

import java.time.Instant

class TemporalExtentSpec extends Specification {

    static TemporalExtent extent(String start, String end) {
        new ImmutableTemporalExtent.Builder().start(start).end(end).build()
    }

    def "bounds are parsed as #description"() {
        expect:
        extent(start, end).toInterval() == Optional.of(Interval.of(Instant.parse(expectedStart), Instant.parse(expectedEnd)))

        where:
        description         | start                       | end                         || expectedStart          | expectedEnd
        "dates"             | "2020-01-01"                | "2020-12-31"                || "2020-01-01T00:00:00Z" | "2020-12-31T23:59:59Z"
        "UTC date-times"    | "2020-01-01T10:00:00Z"      | "2020-01-02T10:00:00.5Z"    || "2020-01-01T10:00:00Z" | "2020-01-02T10:00:00.5Z"
        "offset date-times" | "2020-01-01T10:00:00+01:00" | "2020-01-01T12:00:00+01:00" || "2020-01-01T09:00:00Z" | "2020-01-01T11:00:00Z"
    }

    def "missing bounds are open"() {
        expect:
        extent(null, "2020-12-31").toInterval().get().isUnboundedStart()
        extent("2020-01-01", null).toInterval().get().isUnboundedEnd()
        extent(null, null).toInterval().isEmpty()
    }

    def "invalid extent '#start' / '#end' is rejected"() {
        when:
        extent(start, end)

        then:
        thrown(IllegalStateException)

        where:
        start                  | end
        "1577836800"           | null
        "2020-01-01T10:00:00"  | null
        "2020-13-01"           | null
        "2021-01-01"           | "2020-01-01"
    }

    def "computed must not be combined with bounds"() {
        when:
        new ImmutableTemporalExtent.Builder().start("2020-01-01").computed(true).build()

        then:
        thrown(IllegalStateException)
    }

    def "computed may only be true"() {
        expect:
        new ImmutableTemporalExtent.Builder().computed(true).build().toInterval().isEmpty()

        when:
        new ImmutableTemporalExtent.Builder().computed(false).build()

        then:
        thrown(IllegalStateException)
    }
}
