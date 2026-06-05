/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.util.Locale;
import java.util.Map;
import org.w3c.dom.Element;

public class FesResourceId extends FesExpression {

  private final String id;

  public FesResourceId(String id) {
    super();
    this.id = id;
  }

  @Override
  public void appendXml(FES.VERSION version, Element e, XMLDocument doc) {
    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    Element ex =
        doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.RESOURCEID));
    if (version.isEqual(FES.VERSION._1_1_0)) {
      ex.setAttribute("gml:" + FES.getWord(version, FES.VOCABULARY.RESOURCEID_ATTR), id);
    } else {
      ex.setAttribute(FES.getWord(version, FES.VOCABULARY.RESOURCEID_ATTR), id);
    }
    e.appendChild(ex);
  }

  @Override
  public Map<String, String> toKVP(FES.VERSION version, XMLNamespaceNormalizer nsStore) {
    return ImmutableMap.of(
        FES.getWord(version, FES.VOCABULARY.RESOURCEID_KVP).toUpperCase(Locale.ROOT), id);
  }
}
