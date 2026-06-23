/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class PackedIdSetSpec extends Specification {

    def 'packable ids are deduplicated'() {
        given:
        def set = new PackedIdSet(1000)

        expect:
        set.add("DEHE862010014MLB")
        set.add("DEHE862010014MLC")
        !set.add("DEHE862010014MLB")
        !set.add("DEHE862010014MLC")
        set.size() == 2
    }

    def 'ids with leading zero-characters stay distinct'() {
        given:
        def set = new PackedIdSet(1000)

        expect:
        set.add("A")
        set.add("0A")
        set.add("00A")
        set.size() == 3
        !set.add("0A")
    }

    def 'ids that cannot be packed fall back to a hash'() {
        given:
        def set = new PackedIdSet(1000)

        expect:
        set.add("urn:adv:oid:DEHE862010014MLB")
        !set.add("urn:adv:oid:DEHE862010014MLB")
        set.add("a-very-long-identifier-that-exceeds-the-packing-limit")
        !set.add("a-very-long-identifier-that-exceeds-the-packing-limit")
        set.size() == 2
    }

    def 'the set grows beyond the initial capacity'() {
        given:
        def set = new PackedIdSet(100000)

        when:
        def added = (0..<50000).count { set.add("ID" + it) }
        def readded = (0..<50000).count { set.add("ID" + it) }

        then:
        added == 50000
        readded == 0
        set.size() == 50000
    }

    def 'exceeding the maximum number of entries fails'() {
        given:
        def set = new PackedIdSet(3)
        set.add("A")
        set.add("B")
        set.add("C")

        when:
        set.add("D")

        then:
        thrown IllegalStateException

        and: 'duplicates are still detected'
        !set.add("A")
    }
}
