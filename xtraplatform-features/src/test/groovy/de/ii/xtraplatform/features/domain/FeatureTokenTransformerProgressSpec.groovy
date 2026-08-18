/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class FeatureTokenTransformerProgressSpec extends Specification {

    RecordingJobHook jobHook
    FeatureTokenReader tokenReader
    List<Object> tokens

    def setup() {
        jobHook = new RecordingJobHook()
        FeatureTokenTransformerProgress transformer = new FeatureTokenTransformerProgress(jobHook)
        FeatureQuery query = ImmutableFeatureQuery.builder().type("test").build()
        FeatureEventHandler.ModifiableContext context = transformer.createContext()
                .setQuery(query)
                .setMappings([test: FeatureSchemaFixtures.BIOTOP_MAPPING])
                .setType('test')
                .setIsUseTargetPaths(true)

        tokenReader = new FeatureTokenReader(transformer, context)
        tokens = []
        transformer.init(token -> tokens.add(token))
    }

    def 'init from numberReturned and one update per feature'() {

        given:

        when:

        FeatureTokenFixtures.COLLECTION.forEach(token -> tokenReader.onToken(token))

        then:

        jobHook.inits == [3]
        jobHook.updates == [1, 1, 1]

    }

    def 'no init and no updates without numberReturned'() {

        given:

        when:

        FeatureTokenFixtures.SINGLE_FEATURE.forEach(token -> tokenReader.onToken(token))

        then:

        jobHook.inits == []
        jobHook.updates == []

    }

    def 'tokens pass through unchanged'() {

        given:

        when:

        FeatureTokenFixtures.COLLECTION.forEach(token -> tokenReader.onToken(token))

        then:

        tokens == FeatureTokenFixtures.COLLECTION

    }

    def 'cancellation is requested before the next feature'() {

        given:

        jobHook.cancelRequested = true

        when:

        FeatureTokenFixtures.COLLECTION.forEach(token -> tokenReader.onToken(token))

        then:

        thrown(JobCancelledException)
        jobHook.updates == []

    }
}

class RecordingJobHook implements JobHook {

    List<Integer> inits = []
    List<Integer> updates = []
    boolean cancelRequested = false

    @Override
    void init(int total) {
        inits.add(total)
    }

    @Override
    void update(int delta) {
        updates.add(delta)
    }

    @Override
    boolean isCancelRequested() {
        return cancelRequested
    }
}
