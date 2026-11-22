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

// Decoder.noop decodes the columns of connectors without a format of their own, e.g. SQL
// expressions. The SQL query selects the columns of all properties, while the mapping only holds
// the properties the query selects (AbstractFeatureProvider.createMapping prunes the schema). A
// column of an unselected property has no schema and must be passed on like any other column, not
// rejected as a mapping that is not a value.
class DecoderNoopSpec extends Specification {

    static FeatureSchema property(String name, SchemaBase.Type type = SchemaBase.Type.STRING) {
        return new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(type)
                .sourcePath(name)
                .build()
    }

    static FeatureSchema feature(FeatureSchema... properties) {
        def b = new ImmutableFeatureSchema.Builder()
                .name("test")
                .type(SchemaBase.Type.OBJECT)
                .sourcePath("/test")
        properties.each { b.putPropertyMap(it.getName(), it) }
        return b.build()
    }

    FeatureEventHandler downstream = Mock()

    Decoder.Pipeline pipeline(FeatureSchema schema, List<String> path) {
        FeatureEventHandler.ModifiableContext<FeatureSchema, SchemaMapping> context = ModifiableGenericContext.create()
        context.setMappings([test: SchemaMapping.of(schema)])
        context.setType('test')
        context.setQuery(ImmutableFeatureQuery.builder().type('test').build())
        context.pathTracker().track(path)

        Decoder.Pipeline pipeline = Stub()
        pipeline.context() >> context
        pipeline.downstream() >> downstream

        return pipeline
    }

    def 'a column of a selected value property is passed on'() {
        given:
        def schema = feature(property("id"), property("title"))
        def pipeline = pipeline(schema, ["test", "title"])

        when:
        Decoder.noop("EXPRESSION").decode(new byte[0], pipeline)

        then:
        1 * downstream.onValue({ it.schema().map { s -> s.getName() } == Optional.of("title") })
    }

    def 'a column of a property that the query does not select is passed on without a schema'() {
        given:
        def schema = feature(property("id"), property("title"))
                .accept(new WithoutProperties(["id"], false))
        def pipeline = pipeline(schema, ["test", "title"])

        when:
        Decoder.noop("EXPRESSION").decode(new byte[0], pipeline)

        then:
        noExceptionThrown()
        1 * downstream.onValue({ it.schema().isEmpty() })
    }

    def 'a mapping that is not a value is rejected'() {
        given:
        def schema = feature(property("id"), property("nested", SchemaBase.Type.OBJECT))
        def pipeline = pipeline(schema, ["test", "nested"])

        when:
        Decoder.noop("EXPRESSION").decode(new byte[0], pipeline)

        then:
        def e = thrown(IllegalArgumentException)
        e.message == "Only allowed in value mappings: [EXPRESSION]"
        0 * downstream.onValue(_)
    }
}
