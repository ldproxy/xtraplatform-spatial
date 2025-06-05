/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs.Force;
import java.util.Objects;
import java.util.Optional;

public interface OgcCrs {

  EpsgCrs CRS84 = EpsgCrs.of(4326, Force.LON_LAT);

  String CRS84_URI = "http://www.opengis.net/def/crs/OGC/1.3/CRS84";
  String CRS84_URI_NEW = "http://www.opengis.net/def/crs/OGC/0/CRS84";
  String CRS84_CURIE = "OGC:CRS84";

  EpsgCrs CRS84h = EpsgCrs.of(4979, Force.LON_LAT);

  String CRS84h_URI = "http://www.opengis.net/def/crs/OGC/0/CRS84h";
  String CRS84h_CURIE = "OGC:CRS84h";

  static Optional<EpsgCrs> fromString(String prefixedCode) {
    if (Objects.equals(prefixedCode, CRS84_URI_NEW)) {
      return Optional.of(CRS84);
    }
    if (Objects.equals(prefixedCode, CRS84_URI)) {
      return Optional.of(CRS84);
    }
    if (Objects.equals(prefixedCode, CRS84h_URI)) {
      return Optional.of(CRS84h);
    }

    return Optional.empty();
  }
}
