/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles3d.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.tiles.domain.SeedingOptions;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @langEn ### Seeding
 *     <p>Controls how and when caches are computed.
 * @langDe ### Seeding
 *     <p>Steuert wie und wann Caches berechnet werden.
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableSeedingOptions3d.Builder.class)
public interface SeedingOptions3d extends SeedingOptions {

  @JsonIgnore
  @Nullable
  @Override
  Integer getMaxThreads();

  /**
   * @langEn The maximum number of tiles in a seeding job (S=1, M=4, L=16, XL=64). The tile seeding
   *     is split into multiple jobs to distribute the work across threads and nodes.
   * @langDe Die maximale Anzahl an Tiles in einem Seeding-Job (S=1, M=4, L=16, XL=64). Das Seeding
   *     wird in mehrere Jobs aufgeteilt, um die Arbeit auf Threads und Knoten zu verteilen.
   * @default M
   */
  @Nullable
  @Override
  JobSize getJobSize();
}
