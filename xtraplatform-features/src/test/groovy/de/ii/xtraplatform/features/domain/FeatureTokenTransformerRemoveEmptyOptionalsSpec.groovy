/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain


import spock.lang.Specification

/**
 * @author zahnen
 */
class FeatureTokenTransformerRemoveEmptyOptionalsSpec extends Specification {

    FeatureTokenReader tokenReader
    List<Object> tokens

    def setup() {
        FeatureTokenTransformerRemoveEmptyOptionals mapper = new FeatureTokenTransformerRemoveEmptyOptionals([:])
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

    def 'single feature'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE

    }

    def 'single feature object'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT

    }

    def 'single feature empty optional object'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_EMPTY.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE

    }

    def 'single feature empty required object'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_EMPTY_REQUIRED.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_EMPTY_REQUIRED

    }

    def 'single feature optional object with only null required sub-properties'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_REQUIRED_NULLS.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE

    }

    def 'single feature optional object with a non-null and a null required sub-property'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_REQUIRED_NULLS_PARTIAL.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE_NESTED_OBJECT_REQUIRED_NULLS_PARTIAL_REDUCED

    }

    def 'single feature value array'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY

    }

    def 'single feature empty optional value array'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY_EMPTY.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE

    }

    def 'single feature empty required value array'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY_EMPTY_REQUIRED.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.SINGLE_FEATURE_VALUE_ARRAY_EMPTY_REQUIRED

    }

}
