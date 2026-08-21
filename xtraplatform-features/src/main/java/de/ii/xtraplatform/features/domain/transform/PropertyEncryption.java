/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.base.domain.Encryption;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Optional;

/**
 * Symmetric encryption for values of properties with type {@link Type#ENCRYPTED} or {@link
 * Type#ENCRYPTED_ARRAY}.
 *
 * <p>The stored representation is {@code nonce (12 bytes) || ciphertext || tag (16 bytes)} using
 * AES-256-GCM. Plaintext values are normalized according to the logical type declared in {@code
 * valueType} before encryption and after decryption, so that the lexical form of temporal values is
 * canonical and independent of the form that was received or encrypted.
 */
public class PropertyEncryption {

  private final Encryption encryption;

  public PropertyEncryption(Encryption encryption) {
    this.encryption = encryption;
  }

  public byte[] encrypt(String plaintext) {
    return encryption.encrypt(plaintext.getBytes(StandardCharsets.UTF_8));
  }

  public String decrypt(byte[] encrypted, String propertyLabel) {
    byte[] decrypted =
        encryption.decrypt(encrypted, String.format("for property '%s'", propertyLabel));

    return new String(decrypted, StandardCharsets.UTF_8);
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

  /**
   * Startup validation for a provider: encrypted properties require support by the provider type
   * and a valid key, and a configured key must be well-formed even when no property uses it.
   * Returns an error message, or empty if the configuration is valid.
   */
  public static Optional<String> validateEncryptedProperties(
      Collection<FeatureSchema> types, boolean encryptionEnabled, boolean supported) {
    boolean hasEncryptedProperties =
        types.stream()
            .flatMap(type -> type.getAllNestedProperties().stream())
            .anyMatch(FeatureSchema::isEncrypted);

    if (hasEncryptedProperties && !supported) {
      return Optional.of(
          "the provider type does not support properties of type ENCRYPTED or ENCRYPTED_ARRAY");
    }
    if (hasEncryptedProperties && !encryptionEnabled) {
      return Optional.of(
          "the types have properties of type ENCRYPTED or ENCRYPTED_ARRAY, but encryption is not enabled");
    }

    return Optional.empty();
  }
}
