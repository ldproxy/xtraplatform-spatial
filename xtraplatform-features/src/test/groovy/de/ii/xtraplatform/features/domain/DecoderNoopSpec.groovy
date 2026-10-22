/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext
import spock.lang.Specification

class DecoderNoopSpec extends Specification {

    Decoder.Pipeline pipeline(Optional<FeatureSchema> propertySchema, FeatureEventHandler handler) {
        ModifiableContext context = Stub(ModifiableContext)
        context.schema() >> propertySchema
        Decoder.Pipeline pipeline = Stub(Decoder.Pipeline)
        pipeline.context() >> context
        pipeline.downstream() >> handler
        return pipeline
    }

    def 'the value of a property that is not selected is passed on and not rejected'() {
        given:
        def downstream = Mock(FeatureEventHandler)

        when:
        Decoder.noop("EXPRESSION").decode("18".bytes, pipeline(Optional.empty(), downstream))

        then:
        1 * downstream.onValue(_)
    }

    def 'the value of a value property is passed on'() {
        given:
        def downstream = Mock(FeatureEventHandler)
        def schema = new ImmutableFeatureSchema.Builder().name("grondoppervlakte").type(SchemaBase.Type.INTEGER).build()

        when:
        Decoder.noop("EXPRESSION").decode("18".bytes, pipeline(Optional.of(schema), downstream))

        then:
        1 * downstream.onValue(_)
    }

    def 'an expression for an object property is rejected'() {
        given:
        def schema = new ImmutableFeatureSchema.Builder().name("adres").type(SchemaBase.Type.OBJECT).build()

        when:
        Decoder.noop("EXPRESSION").decode("x".bytes, pipeline(Optional.of(schema), Mock(FeatureEventHandler)))

        then:
        def e = thrown(IllegalArgumentException)
        e.message == "Only allowed in value mappings: [EXPRESSION]"
    }
}
