/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.geometries.domain.Geometry;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new", deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableFeatureToken.Builder.class)
public interface FeatureToken {

  FeatureTokenType getType();

  @Nullable
  String getSource();

  String getTarget();

  @Nullable
  String getValue();

  @Nullable
  Type getValueType();

  @Nullable
  Geometry<?> getGeometry();

  @Nullable
  String getOnlyIf();
}
