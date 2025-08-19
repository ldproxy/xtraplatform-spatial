/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.routes.sql.domain;

import de.ii.xtraplatform.features.domain.FeatureQueryExtension;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface RouteQuery extends FeatureQueryExtension {

  Point getStart();

  Point getEnd();

  List<Point> getWayPoints();

  String getCostColumn();

  String getReverseCostColumn();

  String getMode();

  Optional<Double> getWeight();

  Optional<Double> getHeight();

  Optional<MultiPolygon> getObstacles();

  List<String> getFlags();
}
