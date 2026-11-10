/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.util.List;
import org.w3c.dom.Element;

public class FesOr extends FesExpression {

  private final List<FesExpression> operands;

  public FesOr(List<FesExpression> operands) {
    super();
    this.operands = operands;
  }

  @Override
  public void appendXml(FES.VERSION version, Element e, XMLDocument doc) {
    if (operands.size() == 1) {
      operands.get(0).appendXml(version, e, doc);
    } else if (operands.size() > 1) {
      doc.addNamespace(FES.getNS(version), FES.getPR(version));
      Element ex = doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.OR));
      e.appendChild(ex);

      for (FesExpression expr : operands) {
        expr.appendXml(version, ex, doc);
      }
    }
  }
}
