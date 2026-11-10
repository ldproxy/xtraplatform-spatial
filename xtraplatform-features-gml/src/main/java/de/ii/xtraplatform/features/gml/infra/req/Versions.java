/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import java.util.Objects;

/**
 * @author fischer
 */
@SuppressWarnings("PMD.DataClass")
public class Versions {
  GML.VERSION gmlVersion;
  WFS.VERSION wfsVersion;

  public Versions() {}

  public Versions(WFS.VERSION wfsVersion) {
    this.wfsVersion = wfsVersion;
  }

  public Versions(WFS.VERSION wfsVersion, GML.VERSION gmlVersion) {
    this.wfsVersion = wfsVersion;
    this.gmlVersion = gmlVersion;
  }

  public GML.VERSION getGmlVersion() {
    if (Objects.nonNull(gmlVersion)) {
      return gmlVersion;
    }
    if (Objects.nonNull(wfsVersion)) {
      return wfsVersion.getGmlVersion();
    }
    return null;
  }

  public void setGmlVersion(GML.VERSION gmlVersion) {
    this.gmlVersion = gmlVersion;
  }

  public WFS.VERSION getWfsVersion() {
    return wfsVersion;
  }

  public void setWfsVersion(WFS.VERSION wfsVersion) {
    this.wfsVersion = wfsVersion;
  }
}
