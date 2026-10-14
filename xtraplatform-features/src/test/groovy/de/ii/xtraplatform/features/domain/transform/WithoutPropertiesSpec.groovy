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
import spock.lang.Specification

class WithoutPropertiesSpec extends Specification {

    static FeatureSchema property(String name, SchemaBase.Role role = null) {
        def b = new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(SchemaBase.Type.STRING)
                .sourcePath(name)
        if (role != null) {
            b.role(role)
        }
        return b.build()
    }

    static FeatureSchema object(String name, FeatureSchema... properties) {
        def b = new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(SchemaBase.Type.OBJECT)
                .sourcePath(name == "test" ? "/test" : name)
        properties.each { b.putPropertyMap(it.getName(), it) }
        return b.build()
    }

    static List<String> names(FeatureSchema schema) {
        return schema.getProperties().collect { it.getName() }
    }

    def 'the feature id is kept, if it is not selected'() {
        given:
        FeatureSchema schema = object("test", property("id", SchemaBase.Role.ID), property("name"), property("area"))

        when:
        FeatureSchema result = schema.accept(new WithoutProperties(["area"], false))

        then:
        names(result) == ["id", "area"]
    }

    def 'the feature id is kept, if all other properties are selected'() {
        given:
        FeatureSchema schema = object("test", property("id", SchemaBase.Role.ID), property("name"), property("area"))

        when: 'exclude-properties=id is expressed as the selection of all other properties'
        FeatureSchema result = schema.accept(new WithoutProperties(["name", "area"], false))

        then:
        names(result) == ["id", "name", "area"]
    }

    def 'an id of a nested object does not keep the object'() {
        given:
        FeatureSchema schema = object("test",
                property("id", SchemaBase.Role.ID),
                property("area"),
                object("nested", property("nestedId", SchemaBase.Role.ID), property("value")))

        when:
        FeatureSchema result = schema.accept(new WithoutProperties(["area"], false))

        then:
        names(result) == ["id", "area"]
    }

    def 'without a selection all properties are kept'() {
        given:
        FeatureSchema schema = object("test", property("id", SchemaBase.Role.ID), property("name"))

        when:
        FeatureSchema result = schema.accept(new WithoutProperties(["*"], false))

        then:
        names(result) == ["id", "name"]
    }
}
