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
import de.ii.xtraplatform.features.gml.infra.req.GML;
import de.ii.xtraplatform.features.gml.infra.req.GML.VOCABULARY;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.util.Objects;
import org.w3c.dom.Element;

public class FesTemporalLiteral extends FesExpression {

  protected final String position;
  protected final String beginPosition;
  protected final String endPosition;

  @SuppressWarnings("PMD.NullAssignment")
  public FesTemporalLiteral(String position) {
    super();
    this.position = position;
    this.beginPosition = null;
    this.endPosition = null;
  }

  @SuppressWarnings("PMD.NullAssignment")
  public FesTemporalLiteral(String beginPosition, String endPosition) {
    super();
    this.position = null;
    this.beginPosition = beginPosition;
    this.endPosition = endPosition;
  }

  public boolean isInstant() {
    return Objects.nonNull(position);
  }

  public FesLiteral toInstantLiteral() {
    return new FesLiteral(position);
  }

  @Override
  public void appendXml(VERSION version, Element e, XMLDocument doc) {
    GML.VERSION gmlVersion = version.getGmlVersion();
    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    doc.addNamespace(GML.getNS(gmlVersion), GML.getPR(gmlVersion));

    if (Objects.nonNull(position)) {
      Element ti =
          doc.createElementNS(
              GML.getNS(gmlVersion), GML.getWord(gmlVersion, VOCABULARY.TIME_INSTANT));
      Element tp =
          doc.createElementNS(
              GML.getNS(gmlVersion), GML.getWord(gmlVersion, VOCABULARY.TIME_POSITION));
      tp.setTextContent(position);
      ti.appendChild(tp);
      ti.setAttribute(GML.getWord(gmlVersion, VOCABULARY.GMLID), "TI_1");
      e.appendChild(ti);
    } else {
      Element tp =
          doc.createElementNS(
              GML.getNS(gmlVersion), GML.getWord(gmlVersion, VOCABULARY.TIME_PERIOD));
      Element bp =
          doc.createElementNS(
              GML.getNS(gmlVersion), GML.getWord(gmlVersion, VOCABULARY.BEGIN_POSITION));
      bp.setTextContent(beginPosition);
      tp.appendChild(bp);
      Element ep =
          doc.createElementNS(
              GML.getNS(gmlVersion), GML.getWord(gmlVersion, VOCABULARY.END_POSITION));
      ep.setTextContent(endPosition);
      tp.appendChild(ep);

      tp.setAttribute(GML.getWord(gmlVersion, VOCABULARY.GMLID), "TP_1");
      e.appendChild(tp);
    }
  }
}
