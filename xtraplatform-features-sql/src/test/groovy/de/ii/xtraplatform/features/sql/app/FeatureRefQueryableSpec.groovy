/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.cql.domain.Eq
import de.ii.xtraplatform.cql.domain.Property
import de.ii.xtraplatform.cql.domain.ScalarLiteral
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.ConstantsResolver
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.MappingOperationResolver
import de.ii.xtraplatform.features.domain.MappingRulesDeriver
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.TypesResolver
import de.ii.xtraplatform.features.domain.transform.DefaultRolesResolver
import de.ii.xtraplatform.features.domain.transform.FeatureRefResolver
import de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver
import de.ii.xtraplatform.features.domain.transform.LabelTemplateResolver
import de.ii.xtraplatform.features.json.app.DecoderFactoryJson
import de.ii.xtraplatform.features.sql.domain.ImmutableQueryGeneratorSettings
import de.ii.xtraplatform.features.sql.domain.ImmutableSqlPathDefaults
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import de.ii.xtraplatform.features.sql.domain.SqlPathParser
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping
import spock.lang.Shared
import spock.lang.Specification

/**
 * A queryable for a feature reference must always be filtered against the id property of the
 * reference, no matter how the other properties of the reference are mapped.
 */
class FeatureRefQueryableSpec extends Specification {

    static final String REF_PATH = "[id=related_id]refs{filter=rel_inv='gehoertZuPlan'}/[base_id=id]coretable"

    static final String JOINED = "A.pk IN (SELECT AA.pk FROM coretable AA" +
            " JOIN refs AB ON (AA.id=AB.related_id AND (AB.rel_inv = 'gehoertZuPlan'))" +
            " JOIN coretable AC ON (AB.base_id=AC.id)"

    @Shared
    SqlMappingDeriver mappingDeriver

    @Shared
    FilterEncoderSql filterEncoder

    def setupSpec() {
        def defaults = new ImmutableSqlPathDefaults.Builder().primaryKey("pk").sortKey("pk").build()
        def cql = new CqlImpl()
        def pathParser = new SqlPathParser(defaults, cql,
                Map.of("JSON", new DecoderFactoryJson(), "EXPRESSION", new DecoderFactorySqlExpression()))

        mappingDeriver = new SqlMappingDeriver(pathParser, new ImmutableQueryGeneratorSettings.Builder().build())
        filterEncoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, cql, null)
    }

    /**
     * a feature type on a JSON document column, with a feature reference over a junction table;
     * the title of the reference is optionally mapped to a value inside the JSON document of the
     * referenced feature
     */
    static FeatureSchema bereich(Optional<String> titleSourcePath) {
        def ref = new ImmutableFeatureSchema.Builder()
                .name("gehoertZuPlan")
                .type(SchemaBase.Type.FEATURE_REF)
                .sourcePath(REF_PATH)
                .refType("WP_Plan")
                .putProperties2("id", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("id"))

        titleSourcePath.ifPresent(path -> ref.putProperties2("title",
                new ImmutableFeatureSchema.Builder().type(SchemaBase.Type.STRING).sourcePath(path)))

        return new ImmutableFeatureSchema.Builder()
                .name("WP_Bereich")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/coretable{filter=featuretype='WP_Bereich'}")
                .putProperties2("oid", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("id")
                        .role(SchemaBase.Role.ID))
                .putProperties2("name", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .sourcePath("[JSON]properties/name"))
                .putProperties2("gehoertZuPlan", ref)
                .build()
    }

    static FeatureSchema resolve(FeatureSchema type) {
        Map<String, FeatureSchema> types = Map.of("WP_Bereich", type)
        List<TypesResolver> resolvers = List.of(
                new MappingOperationResolver(true),
                new FeatureRefResolver(Set.of("JSON", "EXPRESSION")),
                new ImplicitMappingResolver(),
                new ConstantsResolver(),
                new LabelTemplateResolver(Optional.empty()),
                new DefaultRolesResolver(),
                new MappingOperationResolver())

        for (TypesResolver resolver : resolvers) {
            int rounds = 0
            while (resolver.needsResolving(types) && rounds < resolver.maxRounds()) {
                types = resolver.resolve(types)
                rounds++
            }
        }

        return types.get("WP_Bereich")
    }

    SqlQueryMapping mapping(Optional<String> titleSourcePath) {
        def resolved = resolve(bereich(titleSourcePath))

        return mappingDeriver.derive(resolved.accept(new MappingRulesDeriver()), resolved).get(0)
    }

    String encode(SqlQueryMapping mapping, String property) {
        return filterEncoder.encode(Eq.of(Property.of(property), ScalarLiteral.of("X")), mapping)
    }

    def 'feature reference queryable filters against the id column: title in the JSON document'() {

        given: "a reference whose title is mapped into the JSON document of the referenced feature"
        def mapping = mapping(Optional.of("[JSON]properties/name"))

        when: "the reference is used as a queryable"
        def actual = encode(mapping, "gehoertZuPlan")

        then: "the id column of the referenced table is filtered, not a value in the JSON document"
        actual == JOINED + " WHERE AC.id = 'X')"
    }

    def 'feature reference queryable filters against the id column: no title'() {

        given: "a reference without a title"
        def mapping = mapping(Optional.empty())

        when: "the reference is used as a queryable"
        def actual = encode(mapping, "gehoertZuPlan")

        then: "the same filter is generated as with a title"
        actual == JOINED + " WHERE AC.id = 'X')"
    }

    def 'feature reference queryable filters against the id column: title in a column'() {

        given: "a reference whose title is mapped to a column of the referenced table"
        def mapping = mapping(Optional.of("label"))

        when: "the reference is used as a queryable"
        def actual = encode(mapping, "gehoertZuPlan")

        then: "the id column of the referenced table is filtered"
        actual == JOINED + " WHERE AC.id = 'X')"
    }

    def 'no object ever claims a value target: title #titleSourcePath'() {

        given: "a feature type with a feature reference"
        def mapping = mapping(titleSourcePath)

        when: "the value targets are resolved"
        def objectValues = mapping.getValueSchemas().findAll { target, schema -> !schema.isValue() }

        then: "none of them is an object, only values can be filtered or sorted on"
        objectValues.isEmpty()

        and: "the reference itself resolves to the id column of the referenced table"
        mapping.getValueColumns().get("gehoertZuPlan").getName() == "id"

        where:
        titleSourcePath << [Optional.of("[JSON]properties/name"), Optional.of("label"), Optional.empty()]
    }

    def 'a value in the JSON document of a referenced feature keeps its path in the document'() {

        given: "a reference whose title is mapped to properties/name of the referenced feature"
        def mapping = mapping(Optional.of("[JSON]properties/name"))

        when: "the title is resolved"
        def schema = mapping.getSchemaForValue("gehoertZuPlan.title")

        then: "the path in the connector is the path in the document, not the property name"
        schema.isPresent()
        mapping.getPathInConnector(schema.get()) == "name"
        mapping.getValueColumns().get("gehoertZuPlan.title").getName() == "properties"
    }
}
