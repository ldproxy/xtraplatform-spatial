/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;
import com.google.common.base.CaseFormat;
import de.ii.xtraplatform.base.domain.JacksonProvider;
import de.ii.xtraplatform.entities.domain.Mergeable;
import de.ii.xtraplatform.entities.domain.maptobuilder.Buildable;
import de.ii.xtraplatform.entities.domain.maptobuilder.BuildableBuilder;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.CUSTOM,
    include = JsonTypeInfo.As.EXISTING_PROPERTY,
    property = "type")
@JsonTypeIdResolver(JacksonProvider.DynamicTypeIdResolver.class)
public interface ExtensionConfiguration
    extends Buildable<ExtensionConfiguration>, Mergeable<ExtensionConfiguration> {

  abstract class Builder implements BuildableBuilder<ExtensionConfiguration> {

    public abstract Builder defaultValues(ExtensionConfiguration defaultValues);
  }

  static String getIdentifier(Class<? extends ExtensionConfiguration> clazz) {
    return CaseFormat.UPPER_CAMEL.to(
        CaseFormat.UPPER_UNDERSCORE,
        clazz
            .getSimpleName()
            .replace("Immutable", "")
            .replace("Configuration", "")
            .replace("Data", ""));
  }

  /**
   * @langEn Always `{@type}`.
   * @langDe Immer `{@type}`.
   */
  @Value.Derived
  default String getType() {
    return getIdentifier(this.getClass());
  }

  /**
   * @langEn Enable the extension?
   * @langDe Soll die Erweiterung aktiviert werden?
   * @default false
   */
  @Nullable
  Boolean getEnabled();

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default boolean isEnabled() {
    return Objects.equals(getEnabled(), true);
  }

  // NOTE: is this really optional, or should we throw an exception when missing?
  @JsonIgnore
  @Value.Auxiliary
  Optional<ExtensionConfiguration> getDefaultValues();

  @Override
  default ExtensionConfiguration mergeInto(ExtensionConfiguration source) {
    return source.getBuilder().from(source).from(this).build();
  }
}
