/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.gml.infra.req.FES.VERSION;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import org.w3c.dom.Element;

public class FesEnvelope extends FesExpression {

  private final BoundingBox boundingBox;

  public FesEnvelope(BoundingBox boundingBox) {
    super();
    this.boundingBox = boundingBox;
  }

  public BoundingBox getBoundingBox() {
    return boundingBox;
  }

  @Override
  public void appendXml(VERSION version, Element e, XMLDocument doc) {}
}
