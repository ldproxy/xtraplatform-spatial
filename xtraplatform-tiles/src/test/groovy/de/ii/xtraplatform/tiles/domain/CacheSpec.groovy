/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.domain

import de.ii.xtraplatform.tiles.domain.Cache.Storage
import de.ii.xtraplatform.tiles.domain.Cache.Type
import spock.lang.Specification

class CacheSpec extends Specification {

    def "the deprecated storage type #deprecated is migrated to #expected"() {
        when:
        Cache cache = new ImmutableCache.Builder().type(Type.IMMUTABLE).storage(deprecated).build()

        then:
        cache.getStorage() == expected

        where:
        deprecated      | expected
        Storage.PLAIN   | Storage.PER_TILE
        Storage.MBTILES | Storage.PER_TILESET
    }

    def "the storage type #storage is kept"() {
        when:
        Cache cache = new ImmutableCache.Builder().type(Type.DYNAMIC).storage(storage).seeded(false).build()

        then:
        cache.getStorage() == storage
        !cache.getSeeded()

        where:
        storage << [Storage.PER_TILE, Storage.PER_JOB, Storage.PER_TILESET]
    }
}
