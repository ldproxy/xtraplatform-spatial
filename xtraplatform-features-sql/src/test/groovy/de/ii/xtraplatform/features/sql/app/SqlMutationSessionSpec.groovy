/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.sql.domain.SqlSession
import spock.lang.Specification

/**
 * Locks the two narrow contracts the {@code ogcapi-transactions} executor relies on:
 *
 * <ul>
 *   <li>{@link SqlMutationSession#commit()}, {@link SqlMutationSession#rollback()} and
 *       {@link SqlMutationSession#close()} are pass-throughs to the underlying SQL session — the
 *       executor's atomic / batch flow assumes nothing else happens at that layer.
 *   <li>An unknown {@code featureType} fails fast with {@code IllegalArgumentException} before any
 *       SQL is generated, so a malformed transaction action can't slip through and execute
 *       arbitrary mutator code.
 * </ul>
 *
 * The full insert/replace mutation path (driven via {@code FeatureMutationsSql} +
 * {@code FeatureEncoderSql} + a Reactive stream runner) needs a heavier fixture and is exercised
 * end-to-end via the gvd/alkis transactions smoke configuration.
 */
class SqlMutationSessionSpec extends Specification {

    SqlSession sqlSession

    def setup() {
        sqlSession = Mock(SqlSession)
    }

    private SqlMutationSession buildSession(Map mappings = [:]) {
        // FeatureMutationsSql, CrsTransformerFactory, nativeCrs, nativeTimeZone and the Reactive
        // runner are only touched by the create/update code paths; null is safe for the lifecycle
        // and missing-type cases this spec covers.
        return new SqlMutationSession(sqlSession, mappings, null, null, null, Optional.empty(), null)
    }

    def 'commit delegates to the underlying SqlSession'() {
        given:
        def session = buildSession()

        when:
        session.commit()

        then:
        1 * sqlSession.commit()
        0 * sqlSession.rollback()
        0 * sqlSession.close()
    }

    def 'rollback delegates to the underlying SqlSession'() {
        given:
        def session = buildSession()

        when:
        session.rollback()

        then:
        1 * sqlSession.rollback()
        0 * sqlSession.commit()
        0 * sqlSession.close()
    }

    def 'close delegates to the underlying SqlSession'() {
        given:
        def session = buildSession()

        when:
        session.close()

        then:
        1 * sqlSession.close()
        0 * sqlSession.commit()
        0 * sqlSession.rollback()
    }

    def 'deleteFeature for an unknown feature type fails fast with IAE (no SQL is run)'() {
        given:
        def session = buildSession([:])  // empty mappings

        when:
        session.deleteFeature('unknown_type', '42')

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('unknown_type')
        0 * sqlSession.run(_, _, _)
    }

    def 'retireFeature for an unknown feature type fails fast with IAE'() {
        given:
        def session = buildSession([:])

        when:
        session.retireFeature('unknown_type', '42', java.time.Instant.parse('2026-06-06T10:00:00Z'))

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('unknown_type')
        0 * sqlSession.runReturning(_)
    }

    def 'patchOpenVersion for an unknown feature type fails fast with IAE'() {
        given:
        def session = buildSession([:])

        when:
        session.patchOpenVersion('unknown_type', '42', [], null)

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('unknown_type')
        0 * sqlSession.runReturning(_)
    }

    def 'assertNoConflictingVersion for an unknown feature type fails fast with IAE'() {
        given:
        def session = buildSession([:])

        when:
        session.assertNoConflictingVersion('unknown_type', '42')

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('unknown_type')
        0 * sqlSession.runReturning(_)
    }

    def 'getOpenVersionStart for an unknown feature type fails fast with IAE'() {
        given:
        def session = buildSession([:])

        when:
        session.getOpenVersionStart('unknown_type', '42')

        then:
        def ex = thrown(IllegalArgumentException)
        ex.message.contains('unknown_type')
        0 * sqlSession.runReturning(_)
    }
}
