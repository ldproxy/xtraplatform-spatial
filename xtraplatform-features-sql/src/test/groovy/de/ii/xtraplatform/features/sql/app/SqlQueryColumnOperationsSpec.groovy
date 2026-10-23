/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.SqlDialectGpkg
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation
import spock.lang.Specification

class SqlQueryColumnOperationsSpec extends Specification {

    static def column(String name, SchemaBase.Type type, Map<Operation, String[]> operations) {
        return new ImmutableSqlQueryColumn.Builder()
                .name(name)
                .pathSegment(name)
                .type(type)
                .schemaIndex(0)
                .operations(operations)
                .build()
    }

    static def geometryExpression(Operation encoding) {
        return column("geom", SchemaBase.Type.GEOMETRY, Map.of(
                Operation.EXPRESSION, ['ST_GeomFromText($T$.wkt)'] as String[],
                encoding, [] as String[]))
    }

    def 'a geometry expression is wrapped in the encoding the column carries'() {

        given: 'a sub-decoder expression column whose geometry is encoded as #encoding'
        def col = geometryExpression(encoding)

        when:
        String actual = SqlQueryColumnOperations.getQualifiedColumnResolved("A", col, dialect)

        then:
        actual == expected

        where:
        dialect               | encoding       || expected
        new SqlDialectGpkg()  | Operation.WKB  || "(ST_AsBinary(ST_GeomFromText(A.wkt))) AS geom"
        new SqlDialectGpkg()  | Operation.WKT  || "(ST_AsText(ST_GeomFromText(A.wkt))) AS geom"
        new SqlDialectPgis()  | Operation.WKB  || "(ST_AsBinary(ST_GeomFromText(A.wkt))) AS geom"
        new SqlDialectPgis()  | Operation.WKT  || "(ST_AsText(ST_GeomFromText(A.wkt))) AS geom"
    }

    def 'a caller that excludes the geometry wrapping gets the raw expression'() {

        given: 'the spatial extent query wraps the raw geometry itself, so it excludes WKB and WKT'
        def col = geometryExpression(Operation.WKB)

        when:
        String actual = SqlQueryColumnOperations.getQualifiedColumnResolved(
                "A", col, new SqlDialectGpkg(), Set.of(Operation.WKB, Operation.WKT), false)

        then: 'the expression is not wrapped, so it is not wrapped twice'
        actual == "(ST_GeomFromText(A.wkt)) AS geom"
    }

    def 'FORCE_POLYGON_CCW on the column reaches a geometry expression'() {

        given: 'a geometry expression column that also carries FORCE_POLYGON_CCW'
        def col = column("geom", SchemaBase.Type.GEOMETRY, Map.of(
                Operation.EXPRESSION, ['ST_GeomFromText($T$.wkt)'] as String[],
                Operation.WKB, [] as String[],
                Operation.FORCE_POLYGON_CCW, [] as String[]))

        when:
        String actual = SqlQueryColumnOperations.getQualifiedColumnResolved("A", col, new SqlDialectGpkg())

        then: 'the expression is wrapped the same way a plain geometry column would be'
        actual == "(ST_AsBinary(ST_ForcePolygonCCW(ST_GeomFromText(A.wkt)))) AS geom"
    }

    def 'a plain geometry column and a geometry expression agree on the flags'() {

        given: 'the same operations, once as a plain column and once via an expression'
        def ops = Map.of(Operation.WKB, [] as String[], Operation.FORCE_POLYGON_CCW, [] as String[])
        def plain = column("geom", SchemaBase.Type.GEOMETRY, ops)
        def viaExpression = column("geom", SchemaBase.Type.GEOMETRY,
                ops + [(Operation.EXPRESSION): ['$T$.geom'] as String[]])
        def dialect = new SqlDialectGpkg()

        when:
        String plainSql = SqlQueryColumnOperations.getQualifiedColumnResolved("A", plain, dialect)
        String expressionSql = SqlQueryColumnOperations.getQualifiedColumnResolved("A", viaExpression, dialect)

        then: 'both force counter-clockwise polygons'
        plainSql == "ST_AsBinary(ST_ForcePolygonCCW(A.geom))"
        expressionSql == "(ST_AsBinary(ST_ForcePolygonCCW(A.geom))) AS geom"
    }

    def 'forceLinearizeCurves from the caller reaches a geometry expression'() {

        given:
        def col = column("geom", SchemaBase.Type.GEOMETRY, Map.of(
                Operation.EXPRESSION, ['$T$.geom'] as String[],
                Operation.WKB, [] as String[]))

        when: 'the caller asks for curves to be linearized'
        String actual = SqlQueryColumnOperations.getQualifiedColumnResolved(
                "A", col, new SqlDialectPgis(), Set.of(), true)

        then: 'the dialect that linearizes does so for the expression too'
        actual == "(ST_AsBinary(ST_CurveToLine(A.geom,32,0,1))) AS geom"
    }

    def 'a non-geometry expression is never wrapped'() {

        given:
        def col = column("name", SchemaBase.Type.STRING, Map.of(
                Operation.EXPRESSION, ['json_extract($T$.props, \'$.name\')'] as String[]))

        when:
        String actual = SqlQueryColumnOperations.getQualifiedColumnResolved("A", col, new SqlDialectGpkg())

        then:
        actual == "(json_extract(A.props, '\$.name')) AS name"
    }
}
