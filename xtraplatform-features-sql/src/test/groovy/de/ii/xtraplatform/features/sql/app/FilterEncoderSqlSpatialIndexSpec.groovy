/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlFilterExamples
import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.And
import de.ii.xtraplatform.cql.domain.Bbox
import de.ii.xtraplatform.cql.domain.IsNull
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.SDisjoint
import de.ii.xtraplatform.cql.domain.SIntersects
import de.ii.xtraplatform.cql.domain.SWithin
import de.ii.xtraplatform.cql.domain.SpatialLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchemaFixtures
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRuleFixtures
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSchemaSql
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQueryMapping
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlQuerySchema
import de.ii.xtraplatform.features.sql.domain.SqlDialectGpkg
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

import java.util.function.Function

class FilterEncoderSqlSpatialIndexSpec extends Specification {

    static final String EXACT_WITHIN = "ST_Within(A.location, ST_GeomFromText('POLYGON((-118.0 33.8,-117.9 33.8,-117.9 34.0,-118.0 34.0,-118.0 33.8))',4326))"
    static final String RTREE_LOOKUP = "A.\"id\" IN (SELECT id FROM \"rtree_building_location\" WHERE maxx >= -118.0 AND minx <= -117.9 AND maxy >= 33.8 AND miny <= 34.0)"

    @Shared
    SqlQueryMapping unfaelleMapping

    def setupSpec() {
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(new ImmutableSqlPathDefaults.Builder().build(), cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        def schema = FeatureSchemaFixtures.fromYaml("strassen_unfaelle2")
        def resolved = schema.accept(new MappingOperationResolver(), List.of())
        unfaelleMapping = mappingDeriver.derive(MappingRuleFixtures.fromYaml("strassen_unfaelle2"), resolved).get(0)
    }

    static FilterEncoderSql encoder(dialect, Map<String, String> spatialIndexes) {
        return new FilterEncoderSql(OgcCrs.CRS84, dialect, null, null, new CqlImpl(), List.of(), null,
                { type -> Optional.empty() } as Function, spatialIndexes)
    }

    def 'gpkg, indexed geometry column on the main table: r-tree lookup is added as a conjunct'() {

        given: 'a GeoPackage provider whose geometry column has an r-tree'
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))

        when: 'a bbox filter on that column is encoded'
        String actual = filterEncoder.encode(CqlFilterExamples.EXAMPLE_15, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then: 'the r-tree is queried in addition to the exact predicate'
        actual == "(${RTREE_LOOKUP} AND ${EXACT_WITHIN})"
    }

    def 'gpkg, geometry column without an r-tree: predicate is unchanged'() {

        given:
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("other_table.location", "id"))

        when:
        String actual = filterEncoder.encode(CqlFilterExamples.EXAMPLE_15, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then:
        actual == EXACT_WITHIN
    }

    def 'table and column are matched case-insensitively, as sqlite compares identifiers'() {

        given: 'a source path that capitalises the column differently than the database reports it'
        def schema = new ImmutableSchemaSql.Builder()
                .name("Building")
                .type(SchemaBase.Type.OBJECT)
                .sortKey("id")
                .addProperties(new ImmutableSchemaSql.Builder()
                        .name("Location")
                        .sourcePath("Location")
                        .type(SchemaBase.Type.GEOMETRY)
                        .parentPath(["Building"])
                        .build())
                .build()
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))

        when:
        def filter = SWithin.of(Property.of("Location"),
                SpatialLiteral.of(Bbox.of(-118.0d, 33.8d, -117.9d, 34.0d, OgcCrs.CRS84)))
        String actual = filterEncoder.encode(filter, schema)

        then: 'the r-tree of that column is still found'
        actual == "(A.\"id\" IN (SELECT id FROM \"rtree_Building_Location\" WHERE maxx >= -118.0 AND minx <= -117.9 AND maxy >= 33.8 AND miny <= 34.0)" +
                " AND ST_Within(A.Location, ST_GeomFromText('POLYGON((-118.0 33.8,-117.9 33.8,-117.9 34.0,-118.0 34.0,-118.0 33.8))',4326)))"
    }

    def 'postgis: no conjunct is added, ST_Within consults the index by itself'() {

        given:
        def filterEncoder = encoder(new SqlDialectPgis(), Map.of("building.location", "id"))

        when:
        String actual = filterEncoder.encode(CqlFilterExamples.EXAMPLE_15, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then:
        actual == EXACT_WITHIN
    }

    def 'S_DISJOINT is not accelerated, a disjoint geometry may lie outside the bbox'() {

        given:
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))
        def filter = SDisjoint.of(Property.of("location"),
                SpatialLiteral.of(Bbox.of(-118.0, 33.8, -117.9, 34.0, OgcCrs.CRS84)))

        when:
        String actual = filterEncoder.encode(filter, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then: 'the exact predicate is emitted on its own'
        actual == "ST_Disjoint(A.location, ST_GeomFromText('POLYGON((-118.0 33.8,-117.9 33.8,-117.9 34.0,-118.0 34.0,-118.0 33.8))',4326))"
    }

    def 'S_DISJOINT and NOT S_INTERSECTS mean the same and are encoded the same way'() {

        given: 'the same predicate, spelled both ways'
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))
        def bbox = SpatialLiteral.of(Bbox.of(-118.0d, 33.8d, -117.9d, 34.0d, OgcCrs.CRS84))

        when:
        String disjoint = filterEncoder.encode(
                SDisjoint.of(Property.of("location"), bbox), QuerySchemaFixtures.SIMPLE_GEOMETRY)
        String notIntersects = filterEncoder.encode(
                Not.of(SIntersects.of(Property.of("location"), bbox)), QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then: 'neither consults the r-tree, so the two spellings cannot disagree'
        !disjoint.contains("rtree_")
        !notIntersects.contains("rtree_")
    }

    def 'under NOT the conjunct is dropped, a NULL geometry has no r-tree entry'() {

        given:
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))

        when:
        String actual = filterEncoder.encode(Not.of(CqlFilterExamples.EXAMPLE_15), QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then: 'the exact predicate is negated on its own, as it was before'
        actual == "NOT (${EXACT_WITHIN})"
    }

    def 'a negation elsewhere in the filter does not stop the conjunct'() {

        given: 'a filter that negates an unrelated predicate and intersects the geometry'
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id"))
        def filter = And.of(Not.of(IsNull.of(Property.of("location"))), CqlFilterExamples.EXAMPLE_15)

        when:
        String actual = filterEncoder.encode(filter, QuerySchemaFixtures.SIMPLE_GEOMETRY)

        then:
        actual == "(A.location IS NOT NULL AND (${RTREE_LOOKUP} AND ${EXACT_WITHIN}))"
    }

    def 'geometry reached through a join is not accelerated, the conjunct would address the wrong table'() {

        given: 'the geometry lives in a joined table, so the predicate goes inside a semi-join'
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("building.location", "id", "geometry.location", "id"))

        when:
        String actual = filterEncoder.encode(CqlFilterExamples.EXAMPLE_16, QuerySchemaFixtures.JOINED_GEOMETRY)

        then:
        actual == "A.id IN (SELECT AA.id FROM building AA JOIN geometry AB ON (AA.id=AB.id) WHERE ST_Intersects(AB.location, ST_GeomFromText('POLYGON((-10.0 -10.0,10.0 -10.0,10.0 10.0,-10.0 -10.0))',4326)))"
    }

    def 'the mapping based encoder, which the items query uses, is accelerated too'() {

        given: 'a filter on the primary geometry of a mapping whose column has an r-tree'
        def filterEncoder = encoder(new SqlDialectGpkg(), Map.of("unfaelle_point.geom", "fid"))
        def filter = SIntersects.of(Property.of("geometry"),
                SpatialLiteral.of(Bbox.of(7.0, 50.0, 7.1, 50.1, OgcCrs.CRS84)))

        when:
        String actual = filterEncoder.encode(filter, unfaelleMapping)

        then:
        actual == "(A.\"fid\" IN (SELECT id FROM \"rtree_unfaelle_point_geom\" WHERE maxx >= 7.0 AND minx <= 7.1 AND maxy >= 50.0 AND miny <= 50.1) AND " +
                "ST_Intersects(A.geom, ST_GeomFromText('POLYGON((7.0 50.0,7.1 50.0,7.1 50.1,7.0 50.1,7.0 50.0))',4326)))"
    }

    def 'a geometry expression stays a geometry in a spatial predicate'() {

        given: 'a WKB-encoded geometry that is derived from an expression'
        def geometry = new ImmutableSqlQueryColumn.Builder()
                .name("geom")
                .pathSegment("geom")
                .type(SchemaBase.Type.GEOMETRY)
                .role(SchemaBase.Role.PRIMARY_GEOMETRY)
                .operations(Map.of(
                        SqlQueryColumn.Operation.CONNECTOR, ["EXPRESSION"] as String[],
                        SqlQueryColumn.Operation.EXPRESSION, ["ST_Force2D(\$T\$.geom)"] as String[],
                        SqlQueryColumn.Operation.WKB, [] as String[]))
                .schemaIndex(0)
                .build()
        def table = new ImmutableSqlQuerySchema.Builder()
                .name("airports")
                .pathSegment("airports")
                .addColumns(geometry)
                .build()
        def geometrySchema = new ImmutableFeatureSchema.Builder()
                .name("geom")
                .type(SchemaBase.Type.GEOMETRY)
                .role(SchemaBase.Role.PRIMARY_GEOMETRY)
                .build()
        def mapping = new ImmutableSqlQueryMapping.Builder()
                .addTables(table)
                .putValueTables("geom", table)
                .putValueColumns("geom", geometry)
                .putValueSchemas("geom", geometrySchema)
                .build()
        def filterEncoder = encoder(new SqlDialectPgis(), Map.of())
        def filter = SIntersects.of(Property.of("geom"),
                SpatialLiteral.of(Bbox.of(7.0, 50.0, 7.1, 50.1, OgcCrs.CRS84)))

        when:
        String actual = filterEncoder.encode(filter, mapping)

        then: 'the output-only WKB wrapper is not passed to ST_Intersects'
        actual == "ST_Intersects((ST_Force2D(A.geom)) , " +
                "ST_GeomFromText('POLYGON((7.0 50.0,7.1 50.0,7.1 50.1,7.0 50.1,7.0 50.0))',4326))"
    }
}
