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
import de.ii.xtraplatform.features.gml.infra.req.FES.VERSION;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocumentFactory;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.ParserConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

public class FesFilter extends FesExpression {

  private static final Logger LOGGER = LoggerFactory.getLogger(FesFilter.class);

  private final List<FesExpression> expressions;

  public FesFilter(List<FesExpression> expressions) {
    super();
    this.expressions = expressions;
  }

  @Override
  public void appendXml(FES.VERSION version, Element e, XMLDocument doc) {

    if (expressions.isEmpty()) {
      return;
    }

    doc.addNamespace(FES.getNS(version), FES.getPR(version));

    Element ex =
        doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.FILTER));

    for (FesExpression expr : expressions) {
      expr.appendXml(version, ex, doc);
    }

    if (ex.getChildNodes().getLength() > 0) {
      e.appendChild(ex);
    }
  }

  @Override
  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  public Map<String, String> toKVP(VERSION version, XMLNamespaceNormalizer nsStore) {
    // check if the first level expression is BBOX
    try {
      FesBBox bbox = (FesBBox) expressions.get(0);
      if (bbox != null) {

        return bbox.toKVP(version, nsStore);
      }
    } catch (ClassCastException ex) {
      // ignore
    }

    // check if the first level expression is ResourceId
    try {
      FesResourceId resid = (FesResourceId) expressions.get(0);
      if (resid != null) {

        return resid.toKVP(version, nsStore);
      }
    } catch (ClassCastException ex) {
      // ignore
    }

    if (expressions.get(0) == null) {
      return ImmutableMap.of();
    }

    try {
      XMLDocumentFactory documentFactory = new XMLDocumentFactory(nsStore);
      XMLDocument doc = documentFactory.newDocument();
      doc.addNamespace(FES.getNS(version), FES.getPR(version));
      Element e =
          doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.FILTER));
      doc.appendChild(e);

      expressions.get(0).appendXml(version, e, doc);

      if (!e.hasChildNodes()) {
        return ImmutableMap.of();
      }

      // attach the namespace(s) for the PropertyName value
      for (String uri : nsStore.xgetNamespaceUris()) {
        e.setAttribute("xmlns:" + uri, nsStore.getNamespaceURI(uri));
      }

      String filter = doc.toString(false);
      filter = filter.replace("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>", "");

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("FES filter: {}", filter);
      }

      filter = "(" + filter + ")";

      return ImmutableMap.of("FILTER", filter);
    } catch (ParserConfigurationException e) {
      throw new IllegalStateException(e);
    }
  }
}
