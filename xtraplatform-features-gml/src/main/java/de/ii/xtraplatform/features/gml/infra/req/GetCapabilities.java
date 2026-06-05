/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocumentFactory;
import java.util.Map;
import org.w3c.dom.Element;

/**
 * @author fischer
 */
public class GetCapabilities implements WfsOperation {

  private final WFS.VERSION version;

  @SuppressWarnings("PMD.NullAssignment")
  public GetCapabilities() {
    this.version = null;
  }

  public GetCapabilities(WFS.VERSION version) {
    this.version = version;
  }

  @Override
  public WFS.OPERATION getOperation() {
    return WFS.OPERATION.GET_CAPABILITES;
  }

  @Override
  public XMLDocument asXml(XMLDocumentFactory documentFactory, Versions versions) {
    if (versions.getWfsVersion() == null) {
      versions.setWfsVersion(WFS.VERSION._1_1_0);
    }

    XMLDocument doc = documentFactory.newDocument();
    doc.addNamespace(WFS.getNS(versions.getWfsVersion()), WFS.getPR(versions.getWfsVersion()));
    Element oper =
        doc.createElementNS(
            WFS.getNS(versions.getWfsVersion()), getOperationName(versions.getWfsVersion()));
    doc.appendChild(oper);

    if (version != null) {
      oper.setAttribute(WFS.getWord(version, WFS.VOCABULARY.VERSION), version.toString());
    }

    oper.setAttribute("service", "WFS");

    return doc;
  }

  @Override
  public Map<String, String> asKvp(XMLDocumentFactory documentFactory, Versions versions) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put("REQUEST", this.getOperation().toString());
    builder.put("SERVICE", "WFS");

    if (version != null) {
      builder.put("VERSION", version.toString());
    } else if (versions.getWfsVersion() != null) {
      builder.put("VERSION", versions.getWfsVersion().toString());
    }

    return builder.build();
  }
}
