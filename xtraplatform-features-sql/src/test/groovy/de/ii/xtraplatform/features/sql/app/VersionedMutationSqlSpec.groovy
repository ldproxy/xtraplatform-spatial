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
 *   <li>{@code assertNoConflictingVersion} (Insert pre-flight):
 *       SELECT gated by an id-existence check — any version (open or retired) of
 *       the same id rejects the Insert.
 *   <li>{@code retireFeature} (Replace's retire half):
 *       SET adds {@code _nachfolger_lzi_beg = ts}; WHERE adds
 *       {@code endCol IS NULL AND startCol < ts AND startCol = expectedStart}.
 *   <li>{@code patchOpenVersion} (Update's RETIRE_IN_PLACE):
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

    def 'Insert pre-flight: assertNoConflictingVersion SQL is a plain id-existence check'() {
        when:
        session.assertNoConflictingVersion(FEATURE_TYPE, 'DEABCDEF12345678')

        then:
        1 * sqlSession.runReturning({ String sql ->
            sql.contains("SELECT 1 FROM ${TABLE}") &&
                    sql.contains("${COL_ID} = 'DEABCDEF12345678'") &&
                    sql.contains('LIMIT 1') &&
                    !sql.contains(COL_END) &&
                    !sql.contains(COL_START)
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

    def 'Update clone-and-patch: cloneAndPatchFeature emits SELECT pk, INSERT clone, retire UPDATE'() {
        given:
        // The fixture has no PREDECESSOR_INTERVAL_START role, so no second OLD-start SELECT runs.
        // `text` is a regular scalar; we patch it to verify the literal lands inline in the clone's
        // SELECT (rather than as a separate UPDATE round-trip after retire).
        FeatureTransactions.PropertyUpdate setText =
                new ImmutablePropertyUpdate.Builder()
                        .path(['text'])
                        .value(Optional.of(
                                com.fasterxml.jackson.databind.node.TextNode.valueOf('updated')))
                        .build()

        when:
        session.cloneAndPatchFeature(
                FEATURE_TYPE,
                'DEABCDEF12345678',
                [setText],
                Instant.parse('2025-10-21T05:46:11Z'),
                null,
                Optional.empty())

        then:
        // (1) Capture OLD_PK.
        1 * sqlSession.runReturning({ String sql ->
            sql.startsWith('SELECT id FROM ') &&
                    sql.contains(TABLE) &&
                    sql.contains("${COL_ID} = 'DEABCDEF12345678'") &&
                    sql.contains("${COL_END} IS NULL")
        }) >> ['42']

        then:
        // (2) Clone main row with role-driven overrides + inline patch literal.
        1 * sqlSession.runReturning({ String sql ->
            sql.startsWith("INSERT INTO ${TABLE} (") &&
                    sql.contains(COL_ID) &&
                    sql.contains(COL_START) &&
                    sql.contains(COL_END) &&
                    sql.contains(COL_SUCC) &&
                    sql.contains('text') &&
                    sql.contains('SELECT') &&
                    sql.contains("m.${COL_ID}") &&
                    sql.contains("'2025-10-21T05:46:11Z'") && // start override
                    sql.contains('NULL') &&                    // end + successor
                    sql.contains("'updated'") &&               // patch literal inline
                    sql.contains("WHERE m.id = 42") &&
                    sql.contains('RETURNING id')
        }) >> ['43']

        then:
        // (3) Retire OLD by surrogate PK with the no-backdating guard.
        1 * sqlSession.runReturning({ String sql ->
            sql.startsWith("UPDATE ${TABLE} SET ") &&
                    sql.contains("${COL_END} = '2025-10-21T05:46:11Z'") &&
                    sql.contains("${COL_SUCC} = '2025-10-21T05:46:11Z'") &&
                    sql.contains('WHERE id = 42') &&
                    sql.contains("${COL_END} IS NULL") &&
                    sql.contains("${COL_START} < '2025-10-21T05:46:11Z'") &&
                    sql.contains('RETURNING id')
        }) >> ['42']
    }

    def 'Update clone-and-patch: empty OLD_PK SELECT returns no ids (caller maps to 409/412)'() {
        when:
        def result = session.cloneAndPatchFeature(
                FEATURE_TYPE,
                'UNKNOWN_ID',
                [],
                Instant.parse('2025-10-21T05:46:11Z'),
                null,
                Optional.of(Instant.parse('2025-10-21T05:24:49Z')))

        then:
        // The OLD-PK SELECT carries the expectedStart predicate — its emptiness is the 412 signal.
        1 * sqlSession.runReturning({ String sql ->
            sql.contains('SELECT id FROM ') &&
                    sql.contains("${COL_ID} = 'UNKNOWN_ID'") &&
                    sql.contains("${COL_END} IS NULL") &&
                    sql.contains("${COL_START} = '2025-10-21T05:24:49Z'")
        }) >> []
        // No subsequent SQL runs.
        0 * sqlSession.runReturning(_)
        result.ids.isEmpty()
        result.error.isEmpty()
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
        // Regular scalar column — no role — used by the clone-and-patch test to verify both
        // carry-forward (`m.text`) and inline patch literal (`'updated'`).
        SqlQueryColumn text = plainColumn('text', SchemaBase.Type.STRING)
        SqlQuerySchema main = new ImmutableSqlQuerySchema.Builder()
                .name(TABLE)
                .pathSegment(TABLE)
                .columns([id, start, end, succ, text])
                .writableColumns([id, start, end, succ, text])
                .build()
        return new ImmutableSqlQueryMapping.Builder()
                .addTables(main)
                // patchInternal resolves the end-setting update via getColumnForValue('lzi.end',
                // Scope.W); populate writableTables/writableColumns for the property paths
                // patchOpenVersion / cloneAndPatchFeature need to walk.
                .putWritableTables('lzi.end', main)
                .putWritableColumns('lzi.end', end)
                .putWritableTables('text', main)
                .putWritableColumns('text', text)
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

    private static SqlQueryColumn plainColumn(String name, SchemaBase.Type type) {
        return new ImmutableSqlQueryColumn.Builder()
                .name(name)
                .pathSegment(name)
                .type(type)
                .schemaIndex(0)
                .build()
    }
}
