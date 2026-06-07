/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.FeatureTransactions
import de.ii.xtraplatform.features.domain.ImmutablePropertyUpdate
import de.ii.xtraplatform.features.domain.MappingRule
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlSession
import spock.lang.Specification

import java.time.Instant

/**
 * Drives the three SQL provider Session methods that an atomic
 * Insert → Replace → Update transaction, and asserts the generated
 * SQL contains every predicate the versioning semantics require:
 *
 * <ul>
 *   <li>{@code assertNoConflictingVersion} (Insert pre-flight, §1.5):
 *       SELECT gated by {@code endCol IS NULL OR endCol > ts OR startCol >= ts}.
 *   <li>{@code retireFeature} (Replace's retire half, §1.3/§1.5/§1.6/§1.8):
 *       SET adds {@code _nachfolger_lzi_beg = ts}; WHERE adds
 *       {@code endCol IS NULL AND startCol < ts AND startCol = expectedStart}.
 *   <li>{@code patchOpenVersion} (Update's RETIRE_IN_PLACE, §1.3/§1.5/§1.8):
 *       UPDATE's WHERE adds {@code endCol IS NULL AND startCol < newEnd AND
 *       startCol = expectedStart}.
 * </ul>
 *
 * The actual Insert / Replace-insert SQL flows through
 * {@code FeatureMutationsSql} + {@code FeatureEncoderSql} which need a
 * full schema fixture; those paths are exercised end-to-end via the
 * gvd/alkis transactions smoke harness, not here.
 */
class VersionedMutationSqlSpec extends Specification {

    static final String TABLE = 'o02340'                       // AP_PTO main table
    static final String COL_ID = 'objid'
    static final String COL_START = 'lzi__beg'
    static final String COL_END = 'lzi__endx'
    static final String COL_SUCC = '_nachfolger_lzi_beg'
    static final String FEATURE_TYPE = 'ap_pto'

    SqlSession sqlSession
    SqlMutationSession session

    def setup() {
        sqlSession = Mock(SqlSession)
        Map<String, List<SqlQueryMapping>> mappings = [(FEATURE_TYPE): [buildMapping()]]
        session = new SqlMutationSession(
                sqlSession, mappings, null, null, null, Optional.empty(), null)
    }

    def 'Insert pre-flight: assertNoConflictingVersion SQL includes all three conflict predicates'() {
        when:
        session.assertNoConflictingVersion(
                FEATURE_TYPE, 'DEABCDEF12345678', Instant.parse('2025-10-21T05:24:49Z'))

        then:
        1 * sqlSession.runReturning({ String sql ->
            sql.contains("SELECT 1 FROM ${TABLE}") &&
                    sql.contains("${COL_ID} = 'DEABCDEF12345678'") &&
                    sql.contains("${COL_END} IS NULL") &&
                    sql.contains("${COL_END} > '2025-10-21T05:24:49Z'") &&
                    sql.contains("${COL_START} >= '2025-10-21T05:24:49Z'") &&
                    sql.contains('LIMIT 1')
        }) >> []
    }

    def 'Replace retire: retireFeature SQL sets end + successor and gates on the composite expectedStart'() {
        given:
        // retire v1 (started at 05:24:49Z) using v2's start (05:46:11Z) as
        // both the retirement timestamp AND the successor pointer.
        Instant retireTs = Instant.parse('2025-10-21T05:46:11Z')
        Instant expectedStart = Instant.parse('2025-10-21T05:24:49Z')

        when:
        session.retireFeature(
                FEATURE_TYPE, 'DEABCDEF12345678', retireTs, Optional.of(expectedStart))

        then:
        1 * sqlSession.runReturning({ String sql ->
            sql.startsWith("UPDATE ${TABLE} SET ") &&
                    sql.contains("${COL_END} = '2025-10-21T05:46:11Z'") &&
                    sql.contains("${COL_SUCC} = '2025-10-21T05:46:11Z'") &&
                    sql.contains("${COL_ID} = 'DEABCDEF12345678'") &&
                    sql.contains("${COL_END} IS NULL") &&
                    sql.contains("${COL_START} < '2025-10-21T05:46:11Z'") &&
                    sql.contains("${COL_START} = '2025-10-21T05:24:49Z'") &&
                    sql.contains("RETURNING ${COL_ID}")
        }) >> ['DEABCDEF12345678']
    }

    def 'Update retire-in-place: patchOpenVersion SQL sets end and gates on expectedStart + startCol < newEnd'() {
        given:
        // Update closes v2 (open, started at 05:46:11Z) at 05:46:20Z. The
        // composite rid suffix carries v2's start as the If-Unmodified-Since expectation.
        FeatureTransactions.PropertyUpdate setEnd =
                new ImmutablePropertyUpdate.Builder()
                        .path(['lzi', 'end'])
                        .value(Optional.of(
                                com.fasterxml.jackson.databind.node.TextNode.valueOf(
                                        '2025-10-21T05:46:20Z')))
                        .build()

        when:
        session.patchOpenVersion(
                FEATURE_TYPE,
                'DEABCDEF12345678',
                [setEnd],
                null,
                Optional.of(Instant.parse('2025-10-21T05:46:11Z')))

        then:
        1 * sqlSession.runReturning({ String sql ->
            sql.startsWith("UPDATE ${TABLE} SET ") &&
                    sql.contains("${COL_END} = '2025-10-21T05:46:20Z'") &&
                    sql.contains("${COL_ID} = 'DEABCDEF12345678'") &&
                    sql.contains("${COL_END} IS NULL") &&
                    sql.contains("${COL_START} < '2025-10-21T05:46:20Z'") &&
                    sql.contains("${COL_START} = '2025-10-21T05:46:11Z'") &&
                    sql.contains("RETURNING ${COL_ID}")
        }) >> ['DEABCDEF12345678']
    }

    // ── fixture ────────────────────────────────────────────────────────────

    private static SqlQueryMapping buildMapping() {
        SqlQueryColumn id = column(COL_ID, SchemaBase.Type.STRING, SchemaBase.Role.ID)
        SqlQueryColumn start = column(
                COL_START, SchemaBase.Type.DATETIME, SchemaBase.Role.PRIMARY_INTERVAL_START)
        SqlQueryColumn end = column(
                COL_END, SchemaBase.Type.DATETIME, SchemaBase.Role.PRIMARY_INTERVAL_END)
        SqlQueryColumn succ = column(
                COL_SUCC,
                SchemaBase.Type.DATETIME,
                SchemaBase.Role.SUCCESSOR_INTERVAL_START)
        SqlQuerySchema main = new ImmutableSqlQuerySchema.Builder()
                .name(TABLE)
                .pathSegment(TABLE)
                .columns([id, start, end, succ])
                .writableColumns([id, start, end, succ])
                .build()
        return new ImmutableSqlQueryMapping.Builder()
                .addTables(main)
                // patchInternal resolves the end-setting update via getColumnForValue('lzi.end',
                // Scope.W); populate writableTables/writableColumns for the property paths
                // patchOpenVersion needs to walk.
                .putWritableTables('lzi.end', main)
                .putWritableColumns('lzi.end', end)
                .build()
    }

    private static SqlQueryColumn column(String name, SchemaBase.Type type, SchemaBase.Role role) {
        return new ImmutableSqlQueryColumn.Builder()
                .name(name)
                .pathSegment(name)
                .type(type)
                .role(role)
                .schemaIndex(0)
                .build()
    }
}
