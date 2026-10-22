/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class AuxiliaryRoleSpec extends Specification {

    static ImmutableFeatureSchema.Builder property(String name, SchemaBase.Type type) {
        return new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(type)
                .sourcePath(name)
    }

    def 'an auxiliary property is returnable and queryable, but never receivable'() {
        given:
        def schema = property("buurtcode", SchemaBase.Type.STRING)
                .role(SchemaBase.Role.AUXILIARY)
                .build()

        expect:
        schema.isAuxiliary()
        schema.returnable()
        schema.queryable()
        !schema.receivable()
    }

    def 'a property without the role is receivable'() {
        given:
        def schema = property("naam", SchemaBase.Type.STRING).build()

        expect:
        !schema.isAuxiliary()
        schema.receivable()
    }
}
