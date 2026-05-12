/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform

import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase
import de.ii.xtraplatform.features.domain.SchemaMapping
import spock.lang.Specification

class SchemaTransformerChainAliasSpec extends Specification {

    static FeatureSchema property(String name, String alias = null) {
        def b = new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(SchemaBase.Type.STRING)
                .sourcePath(name)
        if (alias != null) {
            b.alias(alias)
        }
        return b.build()
    }

    static FeatureSchema feature(FeatureSchema... properties) {
        def b = new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/test")
        properties.each { b.putPropertyMap(it.getName(), it) }
        return b.build()
    }

    static String firstPropertyName(FeatureSchema schema) {
        return schema.getProperties().get(0).getName()
    }

    def "no alias: property name unchanged regardless of useAlias"() {
        given:
        def schema = feature(property("anl"))
        def chain = new SchemaTransformerChain(Map.of(), SchemaMapping.of(schema), false, useAlias)

        when:
        def transformed = schema.accept(chain)

        then:
        firstPropertyName(transformed) == "anl"

        where:
        useAlias << [true, false]
    }

    def "alias present, useAlias=false: property name unchanged"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def chain = new SchemaTransformerChain(Map.of(), SchemaMapping.of(schema), false, false)

        when:
        def transformed = schema.accept(chain)

        then:
        firstPropertyName(transformed) == "anl"
    }

    def "alias present, useAlias=true: property name becomes alias"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def chain = new SchemaTransformerChain(Map.of(), SchemaMapping.of(schema), false, true)

        when:
        def transformed = schema.accept(chain)

        then:
        firstPropertyName(transformed) == "anlass"
    }

    def "alias present, useAlias=true, explicit rename at same path: rename wins"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def transformations = Map.of(
                "anl",
                List.of(new ImmutablePropertyTransformation.Builder().rename("custom").build()))
        def chain = new SchemaTransformerChain(transformations, SchemaMapping.of(schema), false, true)

        when:
        def transformed = schema.accept(chain)

        then:
        firstPropertyName(transformed) == "custom"
    }

    def "alias present, useAlias=true, renamePathOnly at same path: alias still wins on name"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def transformations = Map.of(
                "anl",
                List.of(new ImmutablePropertyTransformation.Builder().renamePathOnly("custom").build()))
        def chain = new SchemaTransformerChain(transformations, SchemaMapping.of(schema), false, true)

        when:
        def transformed = schema.accept(chain)

        then:
        firstPropertyName(transformed) == "anlass"
    }
}
