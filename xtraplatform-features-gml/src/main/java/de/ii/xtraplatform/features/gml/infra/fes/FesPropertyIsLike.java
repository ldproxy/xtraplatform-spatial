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

public class FesPropertyIsLike extends FesExpression {

  private final FesLiteral left;
  private final FesLiteral right;

  private String wildCard;
  private String singleChar;
  private String escapeChar;

  public FesPropertyIsLike(FesLiteral left, FesLiteral right) {
    super();
    this.left = replaceWildCard(left);
    this.right = replaceWildCard(right);

    this.wildCard = "*";
    this.singleChar = "#";
    this.escapeChar = "\\";
  }

  public void setWildCard(String wildCard) {
    this.wildCard = wildCard;
  }

  public void setSingleChar(String singleChar) {
    this.singleChar = singleChar;
  }

  public void setEscapeChar(String escapeChar) {
    this.escapeChar = escapeChar;
  }

  @Override
  public void appendXml(VERSION version, Element e, XMLDocument doc) {

    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    Element ex = doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.LIKE));

    ex.setAttribute(FES.getWord(version, FES.VOCABULARY.WILD_CARD), wildCard);
    ex.setAttribute(FES.getWord(version, FES.VOCABULARY.SINGLE_CHAR), singleChar);
    ex.setAttribute(FES.getWord(version, FES.VOCABULARY.ESCAPE_CHAR), escapeChar);

    e.appendChild(ex);

    left.appendXml(version, ex, doc);
    right.appendXml(version, ex, doc);
  }

  private static FesLiteral replaceWildCard(FesLiteral literal) {
    if (literal.getClass().isAssignableFrom(FesLiteral.class)) {
      return new FesLiteral(literal.getValue().replaceAll("%", "*"));
    }
    return literal;
  }
}
