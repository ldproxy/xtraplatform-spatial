/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.transform.PropertyTransformations
import spock.lang.Specification

class FeatureSchemaAliasesSpec extends Specification {

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

    static FeatureSchema object(String name, String alias, FeatureSchema... children) {
        def b = new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(SchemaBase.Type.OBJECT)
        if (alias != null) {
            b.alias(alias)
        }
        children.each { b.putPropertyMap(it.getName(), it) }
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

    static PropertyTransformations base(Map<String, ?> transformations = [:]) {
        return { -> transformations as Map } as PropertyTransformations
    }

    static String renameAt(PropertyTransformations pt, String path) {
        def entries = pt.transformations.get(path)
        return entries == null ? null : entries.find { it.rename.present }?.rename?.orElse(null)
    }

    def "schema with no aliases: transformations map unchanged"() {
        given:
        def schema = feature(property("anl"))
        def input = base()

        when:
        def result = FeatureSchemaAliases.injectAliasRenames(input, schema)

        then:
        result.is(input)
    }

    def "property alias: rename entry is added at the property's full path"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def input = base()

        when:
        def result = FeatureSchemaAliases.injectAliasRenames(input, schema)

        then:
        renameAt(result, "anl") == "anlass"
    }

    def "nested aliases: rename entries are added at each level's full path"() {
        given:
        def schema = feature(
                object("qag", "qualitaetsangaben",
                        object("dpl", "herkunft",
                                property("prs", "gmd:processStep"))))
        def input = base()

        when:
        def result = FeatureSchemaAliases.injectAliasRenames(input, schema)

        then:
        renameAt(result, "qag") == "qualitaetsangaben"
        renameAt(result, "qag.dpl") == "herkunft"
        renameAt(result, "qag.dpl.prs") == "gmd:processStep"
    }

    def "existing transformations are preserved alongside injected aliases"() {
        given:
        def schema = feature(property("anl", "anlass"))
        def input = base(["other.path": []])

        when:
        def result = FeatureSchemaAliases.injectAliasRenames(input, schema)

        then:
        result.transformations.containsKey("other.path")
        renameAt(result, "anl") == "anlass"
    }

    def "feature type's own alias is not injected (only properties)"() {
        given:
        def schema = new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/test")
                .alias("ignored")
                .putPropertyMap("anl", property("anl", "anlass"))
                .build()
        def input = base()

        when:
        def result = FeatureSchemaAliases.injectAliasRenames(input, schema)

        then:
        !result.transformations.containsKey("")
        renameAt(result, "anl") == "anlass"
    }
}
