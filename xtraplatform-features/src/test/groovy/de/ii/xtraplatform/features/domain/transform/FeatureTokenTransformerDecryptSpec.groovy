/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform

import de.ii.xtraplatform.base.app.EncryptionImpl
import de.ii.xtraplatform.base.domain.Encryption
import de.ii.xtraplatform.features.domain.FeatureSchema
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema
import de.ii.xtraplatform.features.domain.SchemaBase
import spock.lang.Specification

import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.SecureRandom

class FeatureTokenTransformerDecryptSpec extends Specification {

    static final byte[] KEY = (0..31).collect { it as byte } as byte[]
    static final byte[] OTHER_KEY = (32..63).collect { it as byte } as byte[]
    static final PropertyEncryption ENCRYPTION = new PropertyEncryption(new EncryptionImpl(Base64.getEncoder().encodeToString(KEY)))

    def 'an encrypted string value is decrypted'() {
        given: 'a transformer and a ciphertext for a string property'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('nof', SchemaBase.Type.STRING)

        when: 'the value is decrypted'
        def plaintext = transformer.decrypt(encrypt(KEY, 'Müller-Lüdenscheidt, Jürgen'), schema)

        then: 'the original value is restored, including non-ASCII characters'
        plaintext == 'Müller-Lüdenscheidt, Jürgen'
    }

    def 'a date value is normalized to the ISO date format'() {
        given: 'a transformer and a ciphertext for a date property'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('geb', SchemaBase.Type.DATE)

        expect: 'a canonical date is passed through unchanged'
        transformer.decrypt(encrypt(KEY, '1970-01-01'), schema) == '1970-01-01'
    }

    def 'a datetime value is normalized to the ISO datetime format'() {
        given: 'a transformer and a ciphertext for a datetime property'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('zpe', SchemaBase.Type.DATETIME)

        expect: 'a canonical datetime is passed through unchanged'
        transformer.decrypt(encrypt(KEY, '2026-06-02T08:13:10Z'), schema) == '2026-06-02T08:13:10Z'
    }

    def 'a decrypted value that is not a valid date is rejected'() {
        given: 'a transformer and a non-ISO plaintext for a date property'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('geb', SchemaBase.Type.DATE)

        when: 'the value is decrypted'
        transformer.decrypt(encrypt(KEY, '01.02.1970'), schema)

        then: 'the error names the property'
        def e = thrown(IllegalStateException)
        e.message.contains('geb')
    }

    def 'a tampered ciphertext is rejected'() {
        given: 'a transformer and a ciphertext with a flipped bit'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('nof', SchemaBase.Type.STRING)
        byte[] encrypted = Base64.decoder.decode(encrypt(KEY, 'Mustermann'))
        encrypted[encrypted.length - 1] = (byte) (encrypted[encrypted.length - 1] ^ 0x01)

        when: 'the value is decrypted'
        transformer.decrypt(Base64.encoder.encodeToString(encrypted), schema)

        then: 'the integrity check fails'
        def e = thrown(IllegalStateException)
        e.message.contains('wrong key or corrupted')
    }

    def 'a ciphertext encrypted with a different key is rejected'() {
        given: 'a transformer and a ciphertext for another key'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('nof', SchemaBase.Type.STRING)

        when: 'the value is decrypted'
        transformer.decrypt(encrypt(OTHER_KEY, 'Mustermann'), schema)

        then: 'decryption fails'
        thrown(IllegalStateException)
    }

    def 'stored values that are not valid ciphertexts are rejected'() {
        given: 'a transformer'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('nof', SchemaBase.Type.STRING)

        when: 'the stored value is not Base64'
        transformer.decrypt('not base64!', schema)

        then: 'it is rejected'
        thrown(IllegalStateException)

        when: 'the stored value is shorter than nonce plus tag'
        transformer.decrypt(Base64.encoder.encodeToString(new byte[16]), schema)

        then: 'it is rejected'
        thrown(IllegalStateException)
    }

    def 'the encryption key is validated'() {
        when: 'a valid Base64 key of 32 bytes is parsed'
        def key = EncryptionImpl.parseKey(Base64.encoder.encodeToString(KEY))

        then: 'the key is returned'
        key == KEY

        when: 'a key with the wrong length is parsed'
        EncryptionImpl.parseKey(Base64.encoder.encodeToString(new byte[16]))

        then: 'it is rejected'
        thrown(IllegalArgumentException)

        when: 'a key that is not Base64 is parsed'
        EncryptionImpl.parseKey('%%%')

        then: 'it is rejected'
        thrown(IllegalArgumentException)
    }

    def 'a value encrypted with EncryptedValues decrypts to the same plaintext'() {
        given: 'the write-side encryption and the read-side transformer'
        def transformer = new FeatureTokenTransformerDecrypt(ENCRYPTION)
        def schema = encryptedSchema('nof', SchemaBase.Type.STRING)

        when: 'a value is encrypted and then decrypted'
        def encrypted = Base64.encoder.encodeToString(ENCRYPTION.encrypt('Müller-Lüdenscheidt, Jürgen'))

        then: 'the roundtrip restores the original value'
        transformer.decrypt(encrypted, schema) == 'Müller-Lüdenscheidt, Jürgen'
    }

    static FeatureSchema encryptedSchema(String name, SchemaBase.Type valueType) {
        return new ImmutableFeatureSchema.Builder()
                .name(name)
                .type(SchemaBase.Type.ENCRYPTED)
                .valueType(valueType)
                .build()
    }

    static String encrypt(byte[] key, String plaintext) {
        def cipher = Cipher.getInstance('AES/GCM/NoPadding')
        def nonce = new byte[12]
        new SecureRandom().nextBytes(nonce)
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, 'AES'), new GCMParameterSpec(128, nonce))
        byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8))
        byte[] value = new byte[nonce.length + ciphertext.length]
        System.arraycopy(nonce, 0, value, 0, nonce.length)
        System.arraycopy(ciphertext, 0, value, nonce.length, ciphertext.length)
        return Base64.encoder.encodeToString(value)
    }
}
