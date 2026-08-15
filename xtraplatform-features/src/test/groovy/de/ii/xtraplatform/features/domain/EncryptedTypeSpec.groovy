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

class EncryptedTypeSpec extends Specification {

    def 'encrypted properties are neither queryable nor sortable'() {
        given: 'an encrypted scalar and an encrypted array property'
        def scalar = schema(Type.ENCRYPTED, Type.DATE)
        def array = schema(Type.ENCRYPTED_ARRAY, Type.STRING)

        expect: 'both are excluded from queryables and sortables by construction'
        !scalar.queryable()
        !scalar.sortable()
        !array.queryable()
        !array.sortable()
    }

    def 'the encrypted array type is an array and both types are values'() {
        expect:
        schema(Type.ENCRYPTED_ARRAY, Type.STRING).isArray()
        !schema(Type.ENCRYPTED, Type.STRING).isArray()
        schema(Type.ENCRYPTED, Type.STRING).isValue()
        schema(Type.ENCRYPTED_ARRAY, Type.STRING).isValue()
        schema(Type.ENCRYPTED, Type.STRING).isEncrypted()
        schema(Type.ENCRYPTED_ARRAY, Type.STRING).isEncrypted()
        !schema(Type.STRING, null).isEncrypted()
    }

    def 'the mapping rule for an encrypted array keeps the encrypted type for the column'() {
        given: 'a feature type with an encrypted array property'
        def type = new ImmutableFeatureSchema.Builder()
                .name('test')
                .type(Type.OBJECT)
                .sourcePath('/test')
                .putProperties2('id', new ImmutableFeatureSchema.Builder()
                        .type(Type.INTEGER)
                        .role(SchemaBase.Role.ID)
                        .sourcePath('id'))
                .putProperties2('tel', new ImmutableFeatureSchema.Builder()
                        .type(Type.ENCRYPTED_ARRAY)
                        .valueType(Type.STRING)
                        .sourcePath('[id=rid]test_tel/tel'))
                .build()

        when: 'the mapping rules are derived'
        def rules = type.accept(new MappingRulesDeriver())

        then: 'the column rule for the array element keeps the ENCRYPTED type and the array target'
        def telRule = rules.find { it.getSource().endsWith('/tel') }
        telRule != null
        telRule.getType() == Type.ENCRYPTED
    }

    def 'the startup validation catches unsupported or misconfigured encrypted properties'() {
        given: 'a type with an encrypted property and a valid key'
        def types = [new ImmutableFeatureSchema.Builder()
                             .name('test')
                             .type(Type.OBJECT)
                             .sourcePath('/test')
                             .putProperties2('nof', new ImmutableFeatureSchema.Builder()
                                     .type(Type.ENCRYPTED)
                                     .valueType(Type.STRING)
                                     .sourcePath('nof'))
                             .build()]
        def validKey = Base64.encoder.encodeToString(new byte[32])
        def validate = de.ii.xtraplatform.features.domain.transform.EncryptedValues.&validateEncryptedProperties

        expect: 'an unsupported provider type is rejected'
        validate(types, Optional.of(validKey), false).get().contains('does not support')

        and: 'a missing key is rejected'
        validate(types, Optional.empty(), true).get().contains('no encryptionKey')

        and: 'a malformed key is rejected even without encrypted properties'
        validate([], Optional.of('%%%'), true).isPresent()

        and: 'a valid configuration passes'
        validate(types, Optional.of(validKey), true).isEmpty()
        validate([], Optional.empty(), false).isEmpty()
    }

    def 'an encrypted array gets the implicit value array wrap'() {
        given: 'an encrypted array property with a junction source path'
        def property = new ImmutableFeatureSchema.Builder()
                .name('tel')
                .type(Type.ENCRYPTED_ARRAY)
                .valueType(Type.STRING)
                .sourcePath('[id=rid]test_tel/tel')
                .build()
        def resolver = new de.ii.xtraplatform.features.domain.transform.ImplicitMappingResolver()

        expect: 'the resolver adds a VALUE_ARRAY wrap, so the decrypted values are wrapped in array tokens'
        resolver.needsResolving(property, false, false, false)
        resolver.resolve(property, []).getTransformations().first().getWrap().get() == Type.VALUE_ARRAY
    }

    static FeatureSchema schema(Type type, Type valueType) {
        def builder = new ImmutableFeatureSchema.Builder()
                .name('prop')
                .type(type)
        if (valueType != null) {
            builder.valueType(valueType)
        }
        return builder.build()
    }
}
