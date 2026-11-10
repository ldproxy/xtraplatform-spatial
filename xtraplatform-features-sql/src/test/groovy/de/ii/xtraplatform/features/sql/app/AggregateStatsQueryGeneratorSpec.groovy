/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Bbox
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.SIntersects
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.cql.domain.TIntersects
import de.ii.xtraplatform.cql.domain.TemporalLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

import java.time.Instant
import java.util.function.Function

class AggregateStatsQueryGeneratorSpec extends Specification {

    static final String INTERSECTS = "ST_Intersects(A.geom, ST_GeomFromText('POLYGON((7.0 50.0,7.1 50.0,7.1 50.1,7.0 50.1,7.0 50.0))',4326))"

    @Shared
    SqlQueryMapping unfaelleMapping

    @Shared
    AggregateStatsQueryGenerator generator

    def setupSpec() {
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(new ImmutableSqlPathDefaults.Builder().build(), cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        def schema = FeatureSchemaFixtures.fromYaml("strassen_unfaelle2")
        def resolved = schema.accept(new MappingOperationResolver(), List.of())
        unfaelleMapping = mappingDeriver.derive(MappingRuleFixtures.fromYaml("strassen_unfaelle2"), resolved).get(0)

        def filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, new CqlImpl(), List.of(), null,
                { type -> Optional.empty() } as Function, Map.of())
        generator = new AggregateStatsQueryGenerator(new SqlDialectPgis(), filterEncoder)
    }

    def 'without additional filters the count is unrestricted'() {

        when:
        String actual = generator.getCountQuery(unfaelleMapping)

        then:
        actual == "SELECT COUNT(*) FROM unfaelle_point A"
    }

    def 'a restriction to a fixed spatial extent becomes the where clause of the count'() {

        given: 'the filter that restricts queries for multiple features to the extent'
        def filter = SIntersects.of(Property.of("geometry"), SpatialLiteral.of(Bbox.of(7.0, 50.0, 7.1, 50.1, OgcCrs.CRS84)))

        when:
        String actual = generator.getCountQuery(unfaelleMapping, List.of(filter))

        then: 'the count matches what queries for multiple features return'
        actual == "SELECT COUNT(*) FROM unfaelle_point A WHERE ${INTERSECTS}"
    }

    def 'spatial and temporal restrictions are combined'() {

        given:
        def spatial = SIntersects.of(Property.of("geometry"), SpatialLiteral.of(Bbox.of(7.0, 50.0, 7.1, 50.1, OgcCrs.CRS84)))
        def temporal = TIntersects.of(Property.of("unfzeit"),
                new TemporalLiteral.Builder(org.threeten.extra.Interval.of(Instant.parse("2019-01-01T00:00:00Z"), Instant.parse("2019-12-31T23:59:59Z"))).build())

        when:
        String actual = generator.getCountQuery(unfaelleMapping, List.of(spatial, temporal))

        then: 'both predicates are part of a single conjunction'
        actual.startsWith("SELECT COUNT(*) FROM unfaelle_point A WHERE (${INTERSECTS} AND ")
        actual.contains("OVERLAPS (TIMESTAMP '2019-01-01T00:00:00Z',TIMESTAMP '2019-12-31T23:59:59Z')")
    }

    def 'the temporal extent of an instant is empty if there is no value'() {

        given:
        def instant = unfaelleMapping.getColumnForPrimaryInstant().get()

        when:
        String actual = generator.getTemporalExtentQuery(unfaelleMapping, instant.first(), instant.second())

        then: 'no row without a value, so that a NULL can only stand for an unbounded start or end'
        actual == "SELECT T.extent_start, T.extent_end FROM (SELECT MIN(A.unfzeit::timestamp(0)) AS extent_start, MAX(A.unfzeit::timestamp(0)) AS extent_end, COUNT(A.unfzeit::timestamp(0)) AS row_count FROM unfaelle_point A) T WHERE T.row_count > 0"
    }

    def 'the temporal extent of an interval is unbounded at the start or end if any start or end is open'() {

        given: 'start and end in the same table'
        def column = unfaelleMapping.getColumnForPrimaryInstant().get()

        when:
        String actual = generator.getTemporalExtentQuery(unfaelleMapping, column.first(), column.second(), column.first(), column.second())

        then: 'a NULL start or end is not ignored by MIN or MAX but makes that side unbounded'
        actual == "SELECT T.extent_start, T.extent_end FROM (SELECT CASE WHEN COUNT(*) > COUNT(A.unfzeit::timestamp(0)) THEN NULL ELSE MIN(A.unfzeit::timestamp(0)) END AS extent_start, CASE WHEN COUNT(*) > COUNT(A.unfzeit::timestamp(0)) THEN NULL ELSE MAX(A.unfzeit::timestamp(0)) END AS extent_end, COUNT(*) AS row_count FROM unfaelle_point A) T WHERE T.row_count > 0"
    }

    def 'the temporal extent of an interval with start and end in different tables'() {

        given:
        def column = unfaelleMapping.getColumnForPrimaryInstant().get()
        def endTable = new ImmutableSqlQuerySchema.Builder().from(column.first()).name("unfaelle_end").build()

        when:
        String actual = generator.getTemporalExtentQuery(unfaelleMapping, column.first(), column.second(), endTable, column.second())

        then:
        actual == "SELECT S.extent_start, E.extent_end FROM (SELECT CASE WHEN COUNT(*) > COUNT(A.unfzeit::timestamp(0)) THEN NULL ELSE MIN(A.unfzeit::timestamp(0)) END AS extent_start, COUNT(*) AS row_count FROM unfaelle_point A) S, (SELECT CASE WHEN COUNT(*) > COUNT(A.unfzeit::timestamp(0)) THEN NULL ELSE MAX(A.unfzeit::timestamp(0)) END AS extent_end FROM unfaelle_point A) E WHERE S.row_count > 0"
    }
}
