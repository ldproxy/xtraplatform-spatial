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

    FeatureTokenReader tokenReader
    List<Object> tokens

    def setup() {
        FeatureTokenTransformerDeduplicate mapper = new FeatureTokenTransformerDeduplicate(false)
        FeatureQuery query = ImmutableFeatureQuery.builder().type("test").build()
        FeatureEventHandler.ModifiableContext context = mapper.createContext()
                .setQuery(query)
                .setMappings([test: FeatureSchemaFixtures.BIOTOP_MAPPING])
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
