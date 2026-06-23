/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app

import de.ii.xtraplatform.cql.app.CqlImpl
import de.ii.xtraplatform.crs.domain.OgcCrs
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableSchemaConstraints
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.sql.domain.SqlDialectPgis
import spock.lang.Shared
import spock.lang.Specification

class FilterEncoderSqlTargetTypesSpec extends Specification {

    @Shared
    FilterEncoderSql encoder = new FilterEncoderSql(OgcCrs.CRS84, new SqlDialectPgis(), null, null, new CqlImpl(), null)

    static ImmutableFeatureSchema.Builder ref() {
        new ImmutableFeatureSchema.Builder().name("ref").type(SchemaBase.Type.FEATURE_REF)
    }

    def 'case 1: a single refType is the only valid target type'() {
        given:
        def schema = ref().refType("target_a").build()

        expect:
        encoder.validTargetTypes(schema) == Optional.of(["target_a"] as Set)
    }

    def 'case 1: refType DYNAMIC is treated as unconstrained'() {
        given:
        def schema = ref().refType("DYNAMIC").build()

        expect:
        encoder.validTargetTypes(schema) == Optional.empty()
    }

    def 'case 2: concat target types are the union of the members'() {
        given:
        def schema = new ImmutableFeatureSchema.Builder()
                .name("ref")
                .type(SchemaBase.Type.FEATURE_REF_ARRAY)
                .concat([
                        ref().name("a").refType("target_a").build(),
                        ref().name("b").refType("target_b").build()
                ])
                .build()

        expect:
        encoder.validTargetTypes(schema) == Optional.of(["target_a", "target_b"] as Set)
    }

    def 'case 2: an open concat member leaves the whole reference unconstrained'() {
        given:
        def schema = new ImmutableFeatureSchema.Builder()
                .name("ref")
                .type(SchemaBase.Type.FEATURE_REF_ARRAY)
                .concat([
                        ref().name("a").refType("target_a").build(),
                        ref().name("b").refType("DYNAMIC").build()
                ])
                .build()

        expect:
        encoder.validTargetTypes(schema) == Optional.empty()
    }

    def 'case 3: a constant on the type sub-property defines the target type'() {
        given:
        def schema = ref()
                .putProperties2("type", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.TYPE)
                        .constantValue("target_c"))
                .build()

        expect:
        encoder.validTargetTypes(schema) == Optional.of(["target_c"] as Set)
    }

    def 'case 3: an enum on the type sub-property defines the valid target types'() {
        given:
        def schema = ref()
                .putProperties2("type", new ImmutableFeatureSchema.Builder()
                        .type(SchemaBase.Type.STRING)
                        .role(SchemaBase.Role.TYPE)
                        .constraints(new ImmutableSchemaConstraints.Builder()
                                .enumValues(["target_d", "target_e"]).build()))
                .build()

        expect:
        encoder.validTargetTypes(schema) == Optional.of(["target_d", "target_e"] as Set)
    }

    def 'case 4: no refType, concat/coalesce or type constraint is unconstrained'() {
        given:
        def schema = ref().build()

        expect:
        encoder.validTargetTypes(schema) == Optional.empty()
    }

    def 'a null schema is unconstrained'() {
        expect:
        encoder.validTargetTypes(null) == Optional.empty()
    }
}
