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
 * ```
 * </code>
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDynamicSubType(superType = ExtensionConfiguration.class, id = "PGIS_TILES")
@JsonDeserialize(builder = ImmutablePgisTilesConfiguration.Builder.class)
public interface PgisTilesConfiguration extends ExtensionConfiguration {

  abstract class Builder extends ExtensionConfiguration.Builder {}

  @Override
  default Builder getBuilder() {
    return new ImmutablePgisTilesConfiguration.Builder();
  }
}
