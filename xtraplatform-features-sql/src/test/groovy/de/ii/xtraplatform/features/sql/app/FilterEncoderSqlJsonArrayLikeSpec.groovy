/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Function
import de.ii.xtraplatform.cql.domain.Not
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.TypesResolver
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

/**
 * ALIKE matches a LIKE pattern against the items of an array. For an array in a JSON document the
 * pattern must be matched against each item, not against the JSON text of the whole array.
 */
class FilterEncoderSqlJsonArrayLikeSpec extends Specification {

    @Shared
    SqlQueryMapping mapping

    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("pk").sortKey("pk").build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))
        def mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())

        def resolved = resolve(featureType())
        mapping = mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved).get(0)
        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
    }

    static FeatureSchema featureType() {
        return new ImmutableFeatureSchema.Builder()
                .name("WP_Grossverbraucher")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/coretable{filter=featuretype='WP_Grossverbraucher'}")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("id")
                        .role(SchemaBase.Role.ID))
                .putProperties2("name", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("[JSON]properties/name"))
                .putProperties2("versorgungsmedium", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.VALUE_ARRAY)
                        .valueType(SchemaBase.Type.STRING)
                        .sourcePath("[JSON]properties/versorgungsmedium"))
                .putProperties2("waermeErzeuger", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.OBJECT_ARRAY)
                        .sourcePath("[JSON]properties/waermeErzeuger")
                        .putProperties2("art", new ImmutableFeatureSchema.Builder()
                                .type(SchemaBase.Type.STRING)
                                .sourcePath("art")))
                .putProperties2("kennung", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.VALUE_ARRAY)
                        .valueType(SchemaBase.Type.STRING)
                        .sourcePath("[pk=coretable_pk]kennungen/kennung"))
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

    static Function alike(String property, String pattern) {
        return Function.of("ALIKE", [Property.of(property), ScalarLiteral.of(pattern)])
    }

    def 'ALIKE on a value array in a JSON document matches each array item'() {

        when:
        def actual = filterEncoder.encode(alike("versorgungsmedium", "10%"), mapping)

        then:
        actual == "EXISTS (SELECT 1 FROM jsonb_array_elements_text(((A.properties -> 'versorgungsmedium'))::jsonb)" +
                " AS alike_item WHERE alike_item LIKE '10%')"
    }

    def 'ALIKE on a value in an array of objects in a JSON document matches each array item'() {

        when:
        def actual = filterEncoder.encode(alike("waermeErzeuger.art", "10%"), mapping)

        then:
        actual == "EXISTS (SELECT 1 FROM jsonb_array_elements_text((jsonb_path_query_array(A.properties::jsonb,'\$.waermeErzeuger.art'))::jsonb)" +
                " AS alike_item WHERE alike_item LIKE '10%')"
    }

    def 'NOT ALIKE on a value array in a JSON document negates the item match'() {

        when:
        def actual = filterEncoder.encode(Not.of(alike("versorgungsmedium", "10%")), mapping)

        then:
        actual == "NOT (EXISTS (SELECT 1 FROM jsonb_array_elements_text(((A.properties -> 'versorgungsmedium'))::jsonb)" +
                " AS alike_item WHERE alike_item LIKE '10%'))"
    }

    def 'ALIKE on a value array in a joined table matches the joined rows'() {

        when:
        def actual = filterEncoder.encode(alike("kennung", "10%"), mapping)

        then:
        actual == "A.pk IN (SELECT AA.pk FROM coretable AA JOIN kennungen AB ON (AA.pk=AB.coretable_pk)" +
                " WHERE AB.kennung::varchar LIKE '10%')"
    }

    def 'ALIKE on a single value in a JSON document matches the value'() {

        when:
        def actual = filterEncoder.encode(alike("name", "10%"), mapping)

        then:
        actual == "(A.properties ->> 'name')::varchar::varchar LIKE '10%'"
    }
}
