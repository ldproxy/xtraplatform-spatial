/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.req.FES.VOCABULARY;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import org.w3c.dom.Element;

public class FesDuring extends FesExpression {

  private final FesValueReference left;
  private final FesTemporalLiteral right;

  public FesDuring(FesValueReference left, FesTemporalLiteral right) {
    super();
    this.left = left;
    this.right = right;
  }

  @Override
  public void appendXml(FES.VERSION version, Element e, XMLDocument doc) {
    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    Element ex = doc.createElementNS(FES.getNS(version), FES.getWord(version, VOCABULARY.DURING));
    e.appendChild(ex);
    left.appendXml(version, ex, doc);
    right.appendXml(version, ex, doc);
  }
}
