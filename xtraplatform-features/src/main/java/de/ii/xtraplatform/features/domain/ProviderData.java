/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.ii.xtraplatform.docs.DocIgnore;
import de.ii.xtraplatform.entities.domain.EntityData;
import java.util.Locale;
import java.util.Optional;
import org.immutables.value.Value;

public interface ProviderData extends EntityData {

  String ENTITY_TYPE = "providers";
  String PROVIDER_TYPE_KEY = "providerType";
  String PROVIDER_SUB_TYPE_KEY = "providerSubType";

  String getProviderType();

  String getProviderSubType();

  @JsonIgnore
  @Value.Derived
  @Override
  default Optional<String> getEntitySubType() {
    return Optional.of(
        String.format("%s/%s", getProviderType(), getProviderSubType()).toLowerCase(Locale.ROOT));
  }

  // We need to add the @Value.Auxiliary annotation here again, otherwise createdAt and lastModified
  // are still included in the hash. This may be a bug in the immutables?
  @DocIgnore
  @Value.Default
  @Value.Auxiliary
  @Override
  default long getCreatedAt() {
    return EntityData.super.getCreatedAt();
  }

  @DocIgnore
  @Value.Default
  @Value.Auxiliary
  @Override
  default long getLastModified() {
    return EntityData.super.getLastModified();
  }
}
