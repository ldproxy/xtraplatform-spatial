/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.Optional;
import java.util.Set;

public interface FeatureInfo {

  String CAPABILITY = "info";

  String getId();

  Optional<EpsgCrs> getCrs();

  Optional<FeatureSchema> getSchema(String type);

  Set<FeatureSchema> getSchemas();

  Set<GeometryType> getGeometryTypes();
}
