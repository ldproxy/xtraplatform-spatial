/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureTransactions
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableMutationResult
import de.ii.xtraplatform.features.domain.SchemaBase
// the rows of a feature and the insert statements use different Tuple types
import de.ii.xtraplatform.base.domain.util.Tuple as RowTuple
import de.ii.xtraplatform.features.domain.Tuple as StatementTuple
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlSession
import spock.lang.Specification

import java.util.function.Consumer
import java.util.function.Supplier

/**
 * Which identifier a create reports. The identifier of a new feature is
 *
 * <ul>
 *   <li>the one the caller states, where it does (a PUT to the URI of the feature),
 *   <li>otherwise the value of the id column in the request body, where the client assigns
 *       identifiers (an ALKIS {@code gml:id} decoded into an {@code objid} column, where the
 *       surrogate primary key that the insert returns is not the identifier of the feature),
 *   <li>otherwise the identifier the insert returned.
 * </ul>
 *
 * The second rule must not apply where the database generates the identifier: a value in the
 * request body is not inserted then, so reporting it names a feature that does not exist. That
 * was the defect of the {@code Location} header of a create (see the ATS test
 * {@code /conf/features/gml-srsname} of OGC API - Features - Part 4, whose GML request body
 * carries the mandatory {@code gml:id}).
 *
 * <p>The rows and the insert statements are stubbed, so the rule is exercised without the
 * encoder and the stream runner.
 */
class CreatedFeatureIdSpec extends Specification {

    static final String RETURNED_ID = 'B.100'
    static final String ID_IN_REQUEST_BODY = 'B.n1'

    SqlSession sqlSession
    FeatureMutationsSql featureMutationsSql

    def setup() {
        sqlSession = Stub(SqlSession)
        sqlSession.runReturning(_ as String) >> [RETURNED_ID]
        featureMutationsSql = Stub(FeatureMutationsSql)
    }

    def 'a generated identifier is the one the insert returns, not the one in the request body'() {
        given: 'a type whose id column is the primary key, so the database generates the identifier'
        SqlQueryMapping mapping = mapping('id', false)

        when: 'a create with an identifier in the request body'
        List<String> ids = reportedIds(mapping, Optional.empty())

        then: 'the value of the request body is not inserted, so it is not the identifier'
        ids == [RETURNED_ID]
    }

    def 'a client-assigned identifier is the value of the id column in the request body'() {
        given: 'a type whose id column is not the primary key (e.g. an ALKIS objid)'
        SqlQueryMapping mapping = mapping('objid', false)

        when:
        List<String> ids = reportedIds(mapping, Optional.empty())

        then: 'the surrogate primary key that the insert returns is not the identifier'
        ids == [ID_IN_REQUEST_BODY]
    }

    def 'an identifier the client assigns with {generated=false} is taken from the request body'() {
        given: 'the id column is the primary key, but it is not generated on insert'
        SqlQueryMapping mapping = mapping('id', true)

        when:
        List<String> ids = reportedIds(mapping, Optional.empty())

        then:
        ids == [ID_IN_REQUEST_BODY]
    }

    def 'the identifier the caller states wins over both'() {
        given: 'a PUT that creates the feature at the URI of the request'
        SqlQueryMapping mapping = mapping('id', false)

        when:
        List<String> ids = reportedIds(mapping, Optional.of('B.42'))

        then:
        ids == ['B.42']
    }

    /** Runs the create path of the mutation session over one stubbed feature row. */
    List<String> reportedIds(SqlQueryMapping mapping, Optional<String> featureId) {
        SqlQuerySchema table = mapping.getMainTable()
        ModifiableSqlRowData row = ModifiableSqlRowData.create()
        // the encoder stores SQL literals, so a string value is quoted
        row.putValues(mapping.getColumnForId().get().second().getName(), "'$ID_IN_REQUEST_BODY'")
        FeatureDataSql feature = ModifiableFeatureDataSql.create().setMapping(mapping)
        feature.addRows(RowTuple.of(table, row))

        featureMutationsSql.createInstanceInserts(_, _, _, _, _) >> [
                ({ ->
                    StatementTuple.of("INSERT INTO buildings (name) VALUES ('Old Mill') RETURNING id;",
                            ({ String id -> } as Consumer<String>))
                } as Supplier)
        ]

        SqlMutationSession session = new SqlMutationSession(
                sqlSession, [buildings: [mapping]], featureMutationsSql, null, null,
                Optional.empty(), null, Optional.empty())
        ImmutableMutationResult.Builder builder = ImmutableMutationResult.builder()
                .type(FeatureTransactions.MutationResult.Type.CREATE)
                .hasFeatures(false)

        // the entry point that derives the id from the mapping and the written row, i.e. the rule
        // itself — not a value the spec computed for it
        session.writeCollectedFeatures(
                mapping,
                [feature],
                new RowCursor(table.getFullPath()),
                featureId,
                OgcCrs.CRS84,
                false,
                builder)

        return builder.build().getIds()
    }

    /**
     * A mapping with one table and one id column.
     *
     * @param idColumn the name of the column of the property with the role ID; 'id' is the
     *     primary key of the table, any other name is a column of its own
     * @param doNotGenerate the flag {@code {generated=false}} on that column
     */
    static SqlQueryMapping mapping(String idColumn, boolean doNotGenerate) {
        SqlQueryColumn column = new ImmutableSqlQueryColumn.Builder()
                .name(idColumn)
                .pathSegment(idColumn)
                .type(SchemaBase.Type.STRING)
                .role(SchemaBase.Role.ID)
                .operations(doNotGenerate
                        ? Map.of(SqlQueryColumn.Operation.DO_NOT_GENERATE, [] as String[])
                        : Map.of())
                .schemaIndex(0)
                .build()
        SqlQuerySchema table = new ImmutableSqlQuerySchema.Builder()
                .name('buildings')
                .pathSegment('buildings')
                .primaryKey('id')
                .addColumns(column)
                .build()

        return new ImmutableSqlQueryMapping.Builder()
                .addTables(table)
                .mainSchema(new ImmutableFeatureSchema.Builder()
                        .name('buildings')
                        .type(SchemaBase.Type.OBJECT)
                        .sourcePath('/buildings')
                        .putProperties2('id', new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.STRING)
                                .sourcePath(idColumn)
                                .role(SchemaBase.Role.ID))
                        .build())
                .putValueTables('id', table)
                .putValueColumns('id', column)
                .build()
    }
}
