/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.req.FES.VERSION;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import org.w3c.dom.Element;

public class FesLiteral extends FesExpression {

  protected final String value;

  public FesLiteral(String value) {
    super();
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public void appendXml(VERSION version, Element e, XMLDocument doc) {
    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    Element ex =
        doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.LITERAL));
    ex.setTextContent(value);
    e.appendChild(ex);
  }
}
