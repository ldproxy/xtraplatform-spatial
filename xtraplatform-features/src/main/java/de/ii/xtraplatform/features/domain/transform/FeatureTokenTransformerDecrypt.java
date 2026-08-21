/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenTransformer;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * Decrypts values of properties with type {@link Type#ENCRYPTED} or {@link Type#ENCRYPTED_ARRAY} on
 * the read path. The value in the token stream is expected to be the Base64 encoding of the stored
 * representation described in {@link PropertyEncryption}.
 */
public class FeatureTokenTransformerDecrypt extends FeatureTokenTransformer {

  private final PropertyEncryption encryptedValues;

  public FeatureTokenTransformerDecrypt(PropertyEncryption encryptedValues) {
    this.encryptedValues = encryptedValues;
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Optional<FeatureSchema> schema = context.schema();

    if (schema.isPresent() && schema.get().isEncrypted() && Objects.nonNull(context.value())) {
      context.setValue(decrypt(context.value(), schema.get()));
    }

    super.onValue(context);
  }

  String decrypt(String encodedValue, FeatureSchema schema) {
    byte[] encrypted;
    try {
      encrypted = Base64.getDecoder().decode(encodedValue);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          String.format(
              "Decryption failed for property '%s': the stored value is not valid Base64.",
              describe(schema)));
    }

    return normalize(encryptedValues.decrypt(encrypted, describe(schema)), schema);
  }

  String normalize(String plaintext, FeatureSchema schema) {
    try {
      return encryptedValues.normalize(
          plaintext, schema.getValueType().orElse(Type.STRING), describe(schema));
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  private static String describe(FeatureSchema schema) {
    String path = schema.getFullPathAsString();
    return path.isEmpty() ? schema.getName() : path;
  }
}
