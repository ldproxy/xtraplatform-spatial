/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.docs.JsonDynamicSubType;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import org.immutables.value.Value;

/**
 * @type PGIS_TILES
 * @langAll <code>
 * ```yaml
 * - type: PGIS_TILES
 *   enabled: true
 *   unsupportedProperties: IGNORE
 * ```
 * </code>
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDynamicSubType(superType = ExtensionConfiguration.class, id = "PGIS_TILES")
@JsonDeserialize(builder = ImmutablePgisTilesConfiguration.Builder.class)
public interface PgisTilesConfiguration extends ExtensionConfiguration {

  enum UnsupportedMode {
    DISABLE,
    WARN,
    IGNORE
  }

  abstract class Builder extends ExtensionConfiguration.Builder {}

  @Override
  default Builder getBuilder() {
    return new ImmutablePgisTilesConfiguration.Builder();
  }

  /**
   * @langEn How should the extension behave when unsupported properties are found in the
   *     configuration. Options are `DISABLE` to disable the extension, `WARN` to log a warning and
   *     `IGNORE` to ignore the properties.
   * @langDe Wie soll sich die Erweiterung verhalten, wenn nicht unterstützte Properties in der
   *     Konfiguration gefunden werden. Mögliche Werte sind `DISABLE` um die Erweiterung zu
   *     deaktivieren, `WARN` um eine Warnung zu loggen und `IGNORE` um die Properties zu
   *     ignorieren.
   * @default DISABLE
   * @since v4.4
   */
  @Value.Default
  default UnsupportedMode getUnsupportedProperties() {
    return UnsupportedMode.DISABLE;
  }
}
