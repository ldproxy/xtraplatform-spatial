/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.legacy;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import de.ii.xtraplatform.base.domain.JacksonProvider;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @author zahnen
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.CUSTOM,
    include = JsonTypeInfo.As.PROPERTY,
    property = "mappingType")
@JsonTypeIdResolver(JacksonProvider.DynamicTypeIdResolver.class)
public interface TargetMapping<T extends Enum<T>> {
  String BASE_TYPE = "general";

  @Nullable
  String getName();

  @Nullable
  T getType();

  @Nullable
  Boolean getEnabled();

  @Nullable
  Integer getSortPriority();

  @Nullable
  String getFormat();

  default TargetMapping mergeCopyWithBase(TargetMapping targetMapping) {
    setBaseMapping(targetMapping);
    return this;
  }

  @JsonIgnore
  boolean isSpatial();

  @JsonIgnore
  @Value.Derived
  default boolean isEnabled() {
    return getEnabled() == null || getEnabled();
  }

  @JsonIgnore
  default boolean isReference() {
    return false;
  }

  @JsonIgnore
  default boolean isReferenceEmbed() {
    return false;
  }

  @JsonIgnore
  default TargetMapping getBaseMapping() {
    return null;
  }

  default void setBaseMapping(TargetMapping targetMapping) {}
}
