/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class SchemaLinkSpec extends Specification {

    static ImmutableFeatureSchema.Builder property(String name, SchemaBase.Type type) {
        return new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(type)
                .sourcePath(name)
    }

    def 'an explicit link is the effective link'() {
        given:
        def schema = property("code", SchemaBase.Type.STRING)
                .link(SchemaLink.of("related", 'https://example.com/register/{{value}}'))
                .build()

        when:
        def effective = schema.getEffectiveLink()

        then:
        effective.isPresent()
        effective.get().getRel() == "related"
        effective.get().getUriTemplate() == 'https://example.com/register/{{value}}'
    }

    def 'a role with a link relation derives the default link'() {
        given:
        def schema = property("vg", SchemaBase.Type.DATETIME)
                .role(role)
                .build()

        when:
        def effective = schema.getEffectiveLink()

        then:
        effective.isPresent()
        effective.get().getRel() == rel
        effective.get().getUriTemplate() == '{{featureUri}}?datetime={{value}}'

        where:
        role                                            | rel
        SchemaBase.Role.PREDECESSOR_INTERVAL_START      | "predecessor-version"
        SchemaBase.Role.SUCCESSOR_INTERVAL_START        | "successor-version"
    }

    def 'an explicit link overrides the role-derived default'() {
        given:
        def schema = property("vg", SchemaBase.Type.DATETIME)
                .role(SchemaBase.Role.PREDECESSOR_INTERVAL_START)
                .link(SchemaLink.of("predecessor-version", '{{featureUri}}?version={{value}}'))
                .build()

        expect:
        schema.getEffectiveLink().get().getUriTemplate() == '{{featureUri}}?version={{value}}'
    }

    def 'properties without a link or link-relation role have no effective link'() {
        expect:
        property("name", SchemaBase.Type.STRING).build().getEffectiveLink().isEmpty()
        property("beg", SchemaBase.Type.DATETIME)
                .role(SchemaBase.Role.PRIMARY_INTERVAL_START)
                .build()
                .getEffectiveLink()
                .isEmpty()
    }

    def 'values of DATETIME-typed properties are normalized to ISO instants'() {
        // normalizeToIso is only applied to properties of type DATETIME; values of DATE-typed
        // properties bypass it and stay dates. The bare-date row covers the defensive case of a
        // DATETIME-typed property carrying a date-only value.
        expect:
        FeatureTokenTransformerPropertyLinks.normalizeToIso(input) == expected

        where:
        input                       | expected
        "2026-05-12T11:46:39Z"      | "2026-05-12T11:46:39Z"
        "2026-05-12 11:46:39+02"    | "2026-05-12T09:46:39Z"
        "2026-05-12 11:46:39"       | "2026-05-12T11:46:39Z"
        "2026-05-12"                | "2026-05-12T00:00:00Z"
        "not-a-date"                | "not-a-date"
    }
}
