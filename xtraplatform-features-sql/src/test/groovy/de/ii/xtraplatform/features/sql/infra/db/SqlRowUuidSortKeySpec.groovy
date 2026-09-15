/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.infra.db

import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryOptions
import de.ii.xtraplatform.features.sql.domain.SqlQueryOptions
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema
import spock.lang.Specification

import java.sql.ResultSet

class SqlRowUuidSortKeySpec extends Specification {

    static SqlQueryOptions options(SqlQuerySchema table) {
        new ImmutableSqlQueryOptions.Builder().tableSchema(table).build()
    }

    SqlQuerySchema table = Stub(SqlQuerySchema)
    SqlQueryOptions opts

    def setup() {
        table.getSortKeys() >> ['coretable.id']
        table.getColumns() >> []
        table.getName() >> 'coretable'
        table.getFullPath() >> ['coretable']
        opts = options(table)
    }

    SqlRowVals row(UUID sortKey) {
        ResultSet rs = Stub(ResultSet)
        rs.getObject(1) >> sortKey
        new SqlRowVals().read(rs, opts)
    }

    def 'uuid sort keys are merged in database order: #casename'() {

        given: 'two rows whose uuid sort keys the database returned in this order'
        def first = row(UUID.fromString(lower))
        def second = row(UUID.fromString(higher))

        when: 'the row merger compares them'
        def forward = first <=> second
        def backward = second <=> first

        then: 'the comparison reproduces the unsigned byte order PostgreSQL sorted them by'
        forward < 0
        backward > 0

        where:
        casename                       | lower                                  | higher
        'both halves below the sign bit'| '00000000-0000-7000-8000-000000000000' | '70000000-0000-7000-8000-000000000000'
        'high bit set on the greater'   | '70000000-0000-7000-8000-000000000000' | 'f0000000-0000-7000-8000-000000000000'
        'high bit set on both'          | '80000000-0000-4000-8000-000000000000' | 'f0000000-0000-4000-8000-000000000000'
        'differs in the low half only'  | '01a09ff2-b54a-7f48-0000-000000000000' | '01a09ff2-b54a-7f48-f000-000000000000'
        'uuidv7 timestamp ordering'     | '01a09ff2-b54a-7f48-8b07-f7f6d5ad7279' | '01a09ff2-b54b-7033-a3b0-d7ff58f3ccff'
    }

    def 'equal uuid sort keys compare equal'() {

        given: 'two rows carrying the same uuid sort key'
        def uuid = UUID.fromString('01a09ff2-b54a-7f48-8b07-f7f6d5ad7279')

        when: 'the row merger compares them'
        def result = row(uuid) <=> row(uuid)

        then: 'neither row sorts before the other'
        result == 0
    }

    def 'java signed ordering would disagree with the database'() {

        given: 'a pair whose order differs between signed and unsigned comparison'
        def low = UUID.fromString('00000000-0000-4000-8000-000000000000')
        def high = UUID.fromString('f0000000-0000-4000-8000-000000000000')

        expect: 'UUID.compareTo() puts the high-bit value first, which is not the database order'
        low.compareTo(high) > 0

        and: 'the row merger puts it last, as the database did'
        (row(low) <=> row(high)) < 0
    }
}
