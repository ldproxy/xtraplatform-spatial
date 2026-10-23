/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.SchemaBase.Type
import spock.lang.Specification

class FeatureTokenTransformerDeduplicateSpec extends Specification {

    static final FeatureSchema VERSIONED = new ImmutableFeatureSchema.Builder()
            .name("versioned")
            .type(Type.OBJECT)
            .sourcePath("/versioned")
            .putProperties2("id", new ImmutableFeatureSchema.Builder()
                    .type(Type.STRING)
                    .role(SchemaBase.Role.ID)
                    .sourcePath("id"))
            .putProperties2("beg", new ImmutableFeatureSchema.Builder()
                    .type(Type.DATETIME)
                    .role(SchemaBase.Role.PRIMARY_INTERVAL_START)
                    .sourcePath("beg"))
            .putProperties2("end", new ImmutableFeatureSchema.Builder()
                    .type(Type.DATETIME)
                    .role(SchemaBase.Role.PRIMARY_INTERVAL_END)
                    .sourcePath("end"))
            .build()

    static final SchemaMapping VERSIONED_MAPPING = new ImmutableSchemaMapping.Builder()
            .targetSchema(VERSIONED)
            .sourcePathTransformer((path, isValue) -> path)
            .build()

    FeatureTokenReader tokenReader
    List<Object> tokens

    def setup() {
        setupWith(FeatureSchemaFixtures.BIOTOP_MAPPING)
    }

    def setupWith(SchemaMapping mapping) {
        FeatureTokenTransformerDeduplicate mapper = new FeatureTokenTransformerDeduplicate(false)
        FeatureQuery query = ImmutableFeatureQuery.builder().type("test").build()
        FeatureEventHandler.ModifiableContext context = mapper.createContext()
                .setQuery(query)
                .setMappings([test: mapping])
                .setType('test')
                .setIsUseTargetPaths(true)

        tokenReader = new FeatureTokenReader(mapper, context)
        tokens = []
        mapper.init(token -> tokens.add(token))
    }

    static List<Object> feature(String id, String kennung) {
        return [
                FeatureTokenType.FEATURE,
                FeatureTokenType.VALUE,
                ["id"],
                id,
                Type.STRING,
                FeatureTokenType.VALUE,
                ["kennung"],
                kennung,
                Type.STRING,
                FeatureTokenType.FEATURE_END
        ]
    }

    static List<Object> collection(List<Object>... features) {
        List<Object> result = [FeatureTokenType.INPUT, true]
        features.each { result.addAll(it) }
        result.add(FeatureTokenType.INPUT_END)
        return result
    }

    def 'distinct features pass through unchanged'() {
        given:
        def input = collection(feature("24", "611320001-1"), feature("25", "611320001-2"))

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == input
    }

    def 'a feature with an already emitted id is dropped'() {
        given:
        def input = collection(
                feature("24", "611320001-1"),
                feature("25", "611320001-2"),
                feature("24", "611320001-1"))

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == collection(feature("24", "611320001-1"), feature("25", "611320001-2"))
    }

    def 'consecutive duplicates collapse to one feature'() {
        given:
        def input = collection(
                feature("24", "611320001-1"),
                feature("24", "611320001-1"),
                feature("24", "611320001-1"))

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == collection(feature("24", "611320001-1"))
    }

    static List<Object> version(String id, String beg) {
        return [
                FeatureTokenType.FEATURE,
                FeatureTokenType.VALUE,
                ["id"],
                id,
                Type.STRING,
                FeatureTokenType.VALUE,
                ["beg"],
                beg,
                Type.DATETIME,
                FeatureTokenType.FEATURE_END
        ]
    }

    def 'versions of the same feature are all emitted'() {
        given: 'a versioned type and two versions sharing the feature id'
        setupWith(VERSIONED_MAPPING)
        def input = collection(
                version("24", "2017-06-13T08:03:16Z"),
                version("24", "2023-12-30T15:46:55Z"))

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == input
    }

    def 'duplicate versions collapse to one feature'() {
        given:
        setupWith(VERSIONED_MAPPING)
        def input = collection(
                version("24", "2017-06-13T08:03:16Z"),
                version("24", "2017-06-13T08:03:16Z"),
                version("24", "2023-12-30T15:46:55Z"))

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == collection(
                version("24", "2017-06-13T08:03:16Z"),
                version("24", "2023-12-30T15:46:55Z"))
    }

    def 'a versioned feature without an interval start is deduplicated by id at feature end'() {
        given: 'features of a versioned type that carry no interval start value'
        setupWith(VERSIONED_MAPPING)
        def noBeg = [
                FeatureTokenType.FEATURE,
                FeatureTokenType.VALUE,
                ["id"],
                "24",
                Type.STRING,
                FeatureTokenType.FEATURE_END
        ]
        def input = collection(noBeg, noBeg)

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == collection(noBeg)
    }

    def 'properties before the id are kept on the first occurrence'() {
        given: 'kennung arrives before id'
        def feature24 = [
                FeatureTokenType.FEATURE,
                FeatureTokenType.VALUE,
                ["kennung"],
                "611320001-1",
                Type.STRING,
                FeatureTokenType.VALUE,
                ["id"],
                "24",
                Type.STRING,
                FeatureTokenType.FEATURE_END
        ]
        def input = collection(feature24, feature24)

        when:
        input.forEach(token -> tokenReader.onToken(token))

        then:
        tokens == collection(feature24)
    }
}
