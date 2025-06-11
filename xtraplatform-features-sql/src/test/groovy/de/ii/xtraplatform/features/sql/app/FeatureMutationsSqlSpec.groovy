/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Ignore
import spock.lang.Specification

import java.util.function.Function

import static de.ii.xtraplatform.features.sql.app.SqlInsertsFixtures.*

/**
 * @author zahnen
 */
//TODO: fix these tests
@Ignore
class FeatureMutationsSqlSpec extends Specification {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureMutationsSqlSpec.class);

    def 'createInserts'() {

        given:

        FeatureMutationsSql inserts = Spy(new FeatureMutationsSql(null, new SqlInsertGenerator2(OgcCrs.CRS84, null, new ImmutableSqlPathDefaults.Builder().build())))

        Map<List<String>, List<Integer>> rows = ImmutableMap.<List<String>, List<Integer>> builder()
                .put(MAIN_M_2_N_SCHEMA.getFullPath(), ImmutableList.of(3))
                .put(MERGE_MERGE_M_2_N_SCHEMA.getFullPath(), ImmutableList.of(2))
                .build()
        RowCursor rowCursor = new RowCursor(FULL.getPath())

        when:

        //List<Function<FeatureSql, Pair<String, Consumer<String>>>> accept = FULL.accept(new FeatureMutationsSql.StatementsVisitor(rows, rowCursor, false));

        inserts.createInstanceInserts(FULL, rows, rowCursor, Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(FULL, feature, [0], Optional.empty(), null)

        //TODO: after merge_merge
        then:
        1 * inserts.createAttributesInserts(MERGE_WITH_CHILDREN, feature, [0, 0], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MERGE_MERGE_WITH_CHILDREN, feature, [0, 0, 0], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MERGE_MERGE_ONE_2_ONE_SCHEMA, feature, [0, 0, 0, 0], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MERGE_MERGE_M_2_N_SCHEMA, feature, [0, 0, 0, 0], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MERGE_MERGE_M_2_N_SCHEMA, feature, [0, 0, 0, 1], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MAIN_M_2_N_SCHEMA, feature, [0, 0], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MAIN_M_2_N_SCHEMA, feature, [0, 1], Optional.empty(), null)

        then:
        1 * inserts.createAttributesInserts(MAIN_M_2_N_SCHEMA, feature, [0, 2], Optional.empty(), null)

        then:
        0 * inserts.createAttributesInserts(_, _, _)

    }

    def 'createInserts: merge'() {

        given:

        FeatureStoreInsertGenerator generator = Mock();
        FeatureMutationsSql inserts = new FeatureMutationsSql(null, generator)
        List<Integer> rows = ImmutableList.of(0, 0, 1)

        when:

        inserts.createAttributesInserts(MERGE_MERGE_SCHEMA, feature, rows, Optional.empty(), null)

        then:

        1 * generator.createInsert(feature, MERGE_MERGE_SCHEMA, rows, Optional.empty(), null) >> Mock(Function)
        0 * _

    }

    def 'createInserts: merge + 1:1'() {

        given:

        FeatureStoreInsertGenerator generator = Mock();
        FeatureMutationsSql inserts = new FeatureMutationsSql(null, generator)
        List<Integer> rows = ImmutableList.of(0, 0, 0, 1)

        when:

        inserts.createAttributesInserts(MERGE_MERGE_ONE_2_ONE_SCHEMA, feature, rows, Optional.empty(), null)

        then:
        1 * generator.createInsert(feature, MERGE_MERGE_ONE_2_ONE_SCHEMA, rows, Optional.empty(), null) >> Mock(Function)

        then:
        1 * generator.createForeignKeyUpdate(feature, MERGE_MERGE_ONE_2_ONE_SCHEMA, rows) >> Mock(Function)

        then:
        0 * _

    }

    def 'createInserts: merge + m:n'() {

        given:

        FeatureStoreInsertGenerator generator = Mock();
        FeatureMutationsSql inserts = new FeatureMutationsSql(null, generator)
        List<Integer> rows = ImmutableList.of(0, 0, 0, 1)

        when:

        inserts.createAttributesInserts(MERGE_MERGE_M_2_N_SCHEMA, feature, rows, Optional.empty(), null)

        then:
        1 * generator.createInsert(feature, MERGE_MERGE_M_2_N_SCHEMA, rows, Optional.empty(), null) >> Mock(Function)

        then:
        1 * generator.createJunctionInsert(feature, MERGE_MERGE_M_2_N_SCHEMA, rows) >> Mock(Function)

        then:
        0 * _

    }

}
