/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collection;
import java.util.Optional;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Symmetric encryption for values of properties with type {@link Type#ENCRYPTED} or {@link
 * Type#ENCRYPTED_ARRAY}.
 *
 * <p>The stored representation is {@code nonce (12 bytes) || ciphertext || tag (16 bytes)} using
 * AES-256-GCM. Plaintext values are normalized according to the logical type declared in {@code
 * valueType} before encryption and after decryption, so that the lexical form of temporal values is
 * canonical and independent of the form that was received or encrypted.
 */
public class EncryptedValues {

  public static final int KEY_LENGTH = 32;
  public static final int NONCE_LENGTH = 12;
  public static final int TAG_LENGTH_BITS = 128;
  private static final String ALGORITHM = "AES";
  private static final String CIPHER = "AES/GCM/NoPadding";

  /**
   * Startup validation for a provider: encrypted properties require support by the provider type
   * and a valid key, and a configured key must be well-formed even when no property uses it.
   * Returns an error message, or empty if the configuration is valid.
   */
  public static Optional<String> validateEncryptedProperties(
      Collection<FeatureSchema> types, Optional<String> encryptionKey, boolean supported) {
    boolean hasEncryptedProperties =
        types.stream()
            .flatMap(type -> type.getAllNestedProperties().stream())
            .anyMatch(FeatureSchema::isEncrypted);

    if (hasEncryptedProperties && !supported) {
      return Optional.of(
          "the provider type does not support properties of type ENCRYPTED or ENCRYPTED_ARRAY");
    }
    if (hasEncryptedProperties && encryptionKey.isEmpty()) {
      return Optional.of(
          "the types have properties of type ENCRYPTED or ENCRYPTED_ARRAY, but no encryptionKey is configured");
    }
    if (encryptionKey.isPresent()) {
      try {
        parseKey(encryptionKey.get());
      } catch (IllegalArgumentException e) {
        return Optional.of(e.getMessage());
      }
    }

    return Optional.empty();
  }

  public static byte[] parseKey(String base64Key) {
    byte[] key;
    try {
      key = Base64.getDecoder().decode(base64Key);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("The encryption key is not valid Base64.");
    }
    if (key.length != KEY_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "The encryption key must be %d bytes long (AES-256), found %d bytes.",
              KEY_LENGTH, key.length));
    }
    return key;
  }

  private final SecretKeySpec key;
  private final Cipher cipher;
  private final SecureRandom random;

  public EncryptedValues(byte[] key) {
    if (key.length != KEY_LENGTH) {
      throw new IllegalArgumentException(
          String.format(
              "The encryption key must be %d bytes long (AES-256), found %d bytes.",
              KEY_LENGTH, key.length));
    }
    this.key = new SecretKeySpec(key, ALGORITHM);
    this.random = new SecureRandom();
    try {
      this.cipher = Cipher.getInstance(CIPHER);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("AES-256-GCM is not available in this runtime.", e);
    }
  }

  public synchronized byte[] encrypt(String plaintext) {
    byte[] nonce = new byte[NONCE_LENGTH];
    random.nextBytes(nonce);
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_LENGTH_BITS, nonce));
      byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
      byte[] encrypted = new byte[NONCE_LENGTH + ciphertext.length];
      System.arraycopy(nonce, 0, encrypted, 0, NONCE_LENGTH);
      System.arraycopy(ciphertext, 0, encrypted, NONCE_LENGTH, ciphertext.length);
      return encrypted;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Encryption failed.", e);
    }
  }

  public synchronized String decrypt(byte[] encrypted, String propertyLabel) {
    if (encrypted.length <= NONCE_LENGTH + TAG_LENGTH_BITS / 8) {
      throw new IllegalStateException(
          String.format(
              "Decryption failed for property '%s': the stored value is too short.",
              propertyLabel));
    }
    try {
      cipher.init(
          Cipher.DECRYPT_MODE,
          key,
          new GCMParameterSpec(TAG_LENGTH_BITS, encrypted, 0, NONCE_LENGTH));
      byte[] plaintext = cipher.doFinal(encrypted, NONCE_LENGTH, encrypted.length - NONCE_LENGTH);
      return new String(plaintext, StandardCharsets.UTF_8);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(
          String.format(
              "Decryption failed for property '%s': wrong key or corrupted value.", propertyLabel));
    }
  }

  public String normalize(String plaintext, Type valueType, String propertyLabel) {
    try {
      if (valueType == Type.DATE) {
        return LocalDate.parse(plaintext).toString();
      }
      if (valueType == Type.DATETIME) {
        return OffsetDateTime.parse(plaintext).toString();
      }
    } catch (DateTimeParseException e) {
      // IllegalArgumentException, so that invalid values in a mutation request are reported as a
      // client error; the read path wraps this into an IllegalStateException, a stored value that
      // does not match the declared valueType is a server-side data error there
      throw new IllegalArgumentException(
          String.format(
              "The value of the encrypted property '%s' is not a valid %s.",
              propertyLabel, valueType));
    }

    return plaintext;
  }
}
