/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Cql
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.TypesResolver
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

class FilterEncoderSqlConcatNestedGeometrySpec extends Specification {

    @Shared
    Cql cql

    @Shared
    List<SqlQueryMapping> mappings

    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("id").sortKey("id").build()
        cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql, Map.of())
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())

        def resolved = resolve(featureType())
        mappings = mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved)
        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
    }

    static FeatureSchema member(String table) {
        new ImmutableFeatureSchema.Builder()
                .name(table)
                .sourcePath("/" + table)
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .sourcePath("id")
                        .type(SchemaBase.Type.INTEGER)
                        .role(SchemaBase.Role.ID))
                .putProperties2("geometry2D", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT)
                        .objectType("BuildingGeometry2D")
                        .putProperties2("geometry", new ImmutableFeatureSchema.Builder()
                                .sourcePath("position")
                                .type(SchemaBase.Type.GEOMETRY)
                                .role(SchemaBase.Role.PRIMARY_GEOMETRY)
                                .geometryType(de.ii.xtraplatform.geometries.domain.GeometryType.ANY)))
                .build()
    }

    static FeatureSchema featureType() {
        return new ImmutableFeatureSchema.Builder()
                .name("building")
                .type(SchemaBase.Type.OBJECT)
                .concat([member("a"), member("b")])
                .build()
    }

    static FeatureSchema resolve(FeatureSchema type) {
        Map<String, FeatureSchema> types = Map.of(type.getName(), type)
        List<TypesResolver> resolvers = List.of(
                new MappingOperationResolver(true),
                new ImplicitMappingResolver(),
                new MappingOperationResolver())
        for (TypesResolver resolver : resolvers) {
            int rounds = 0
            while (resolver.needsResolving(types) && rounds < resolver.maxRounds()) {
                types = resolver.resolve(types)
                rounds++
            }
        }
        return types.get(type.getName())
    }

    def 'A spatial filter on a geometry nested in an object applies to each main table'() {
        expect:
        filterEncoder.encode(cql.read("S_INTERSECTS(geometry2D.geometry, BBOX(6.5,51.2,7.2,51.5))", Cql.Format.TEXT), mappings.get(member)) ==
                "ST_Intersects(A.position, ST_GeomFromText('POLYGON((6.5 51.2,7.2 51.2,7.2 51.5,6.5 51.5,6.5 51.2))',4326))"

        where:
        member << [0, 1]
    }
}
