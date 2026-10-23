/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.transform.WithoutProperties
import spock.lang.Specification

// AbstractFeatureProvider.createMapping() prunes the schema to the query's fields and then applies
// the transformation chain. The order matters: a property selection is expressed in technical
// property names, while the chain renames them (explicitly, or via the alias renames a format with
// useAlias injects). Pruning after the rename matches the selection against names that no longer
// exist, which silently empties the response of every property whose name the rename changed —
// leaving only those whose name it happens to leave alone.
class ProjectionBeforeRenameSpec extends Specification {

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

    static List<String> names(FeatureSchema schema) {
        return schema.getProperties().collect { it.getName() }
    }

    // the alias renames that a format with useAlias contributes, as a schema transformer chain
    static def aliasChain(FeatureSchema schema) {
        def transformations = FeatureSchemaAliases.injectAliasRenames({ -> [:] as Map }, schema)
        return transformations.getSchemaTransformations(null, true)
    }

    def 'pruning before the rename chain keeps the selected properties, under their alias'() {
        given:
        def schema = feature(property("id"), property("fsk", "flurstueckskennzeichen"), property("afl", "amtlicheFlaeche"))

        when:
        def result = schema
                .accept(new WithoutProperties(["id", "fsk"], false))
                .accept(aliasChain(schema))

        then:
        names(result) == ["id", "flurstueckskennzeichen"]
    }

    def 'pruning after the rename chain loses every renamed property — the defect this order prevents'() {
        given:
        def schema = feature(property("id"), property("fsk", "flurstueckskennzeichen"), property("afl", "amtlicheFlaeche"))

        when:
        def result = schema
                .accept(aliasChain(schema))
                .accept(new WithoutProperties(["id", "fsk"], false))

        then: 'only the property whose name the rename left alone survives'
        names(result) == ["id"]
    }

    def 'an empty selection is unaffected by the order'() {
        given:
        def schema = feature(property("id"), property("fsk", "flurstueckskennzeichen"))

        when:
        def result = schema
                .accept(new WithoutProperties([], false))
                .accept(aliasChain(schema))

        then:
        names(result) == ["id", "flurstueckskennzeichen"]
    }

    def 'a schema without aliases is unaffected by the order'() {
        given:
        def schema = feature(property("id"), property("fsk"), property("afl"))

        when:
        def pruneFirst = schema.accept(new WithoutProperties(["id", "fsk"], false)).accept(aliasChain(schema))
        def renameFirst = schema.accept(aliasChain(schema)).accept(new WithoutProperties(["id", "fsk"], false))

        then:
        names(pruneFirst) == ["id", "fsk"]
        names(renameFirst) == ["id", "fsk"]
    }
}
