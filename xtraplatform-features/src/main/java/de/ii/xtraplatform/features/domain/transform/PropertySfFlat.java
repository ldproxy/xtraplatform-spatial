/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.PropertyBase;
import de.ii.xtraplatform.geometries.domain.transcode.jts.GeometryEncoderJts;
import java.util.Optional;
import org.immutables.value.Value;
import org.locationtech.jts.geom.GeometryFactory;

@Value.Modifiable
@Value.Style(set = "*")
public interface PropertySfFlat extends PropertyBase<PropertySfFlat, FeatureSchema> {

  default Optional<org.locationtech.jts.geom.Geometry> getJtsGeometry(
      GeometryFactory geometryFactory) {
    return getGeometry() != null
        ? Optional.of(getGeometry().accept(new GeometryEncoderJts(geometryFactory)))
        : Optional.empty();
  }
}
