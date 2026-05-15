/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Temporal extent with start and end as Unix timestamp in milliseconds. */
@Value.Immutable
@JsonDeserialize(builder = ImmutableTemporalExtent.Builder.class)
public interface TemporalExtent {

  /**
   * Start of the temporal extent as Unix timestamp in milliseconds, or {@code null} for open start.
   */
  @Nullable
  Long getStart();

  /** End of the temporal extent as Unix timestamp in milliseconds, or {@code null} for open end. */
  @Nullable
  Long getEnd();
}
