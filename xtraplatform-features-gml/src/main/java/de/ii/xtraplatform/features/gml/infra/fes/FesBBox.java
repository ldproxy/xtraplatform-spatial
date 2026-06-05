/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.req.FES.VERSION;
import de.ii.xtraplatform.features.gml.infra.req.GML;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.util.Locale;
import java.util.Map;
import org.w3c.dom.Element;

public class FesBBox extends FesExpression {

  private final BoundingBox env;
  private final String geometryPath;

  public FesBBox(BoundingBox env, String geometryPath) {
    super();
    this.geometryPath = geometryPath;
    this.env = env;
  }

  @Override
  public void appendXml(FES.VERSION version, Element e, XMLDocument doc) {

    // LOGGER.debug("BBOX {} {}", FES.getNS(version), FES.getQN(version, FES.VOCABULARY.BBOX));
    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    doc.addNamespace(GML.getNS(version.getGmlVersion()), GML.getPR(version));

    Element bbox =
        doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.BBOX));
    e.appendChild(bbox);

    Element valRef =
        doc.createElementNS(
            FES.getNS(version), FES.getWord(version, FES.VOCABULARY.VALUE_REFERENCE));
    bbox.appendChild(valRef);
    valRef.setTextContent(geometryPath);

    String min = env.getXmin() + " " + env.getYmin();
    String max = env.getXmax() + " " + env.getYmax();

    // only swap if WFS 1.0.0 ...
    if (!version.isGreaterOrEqual(FES.VERSION._2_0_0)) {
      min = env.getYmin() + " " + env.getXmin();
      max = env.getYmax() + " " + env.getXmax();
    }

    Element envelope =
        doc.createElementNS(
            GML.getNS(version.getGmlVersion()),
            GML.getWord(version.getGmlVersion(), GML.VOCABULARY.ENVELOPE));

    if (version.isGreaterOrEqual(FES.VERSION._2_0_0)) {
      envelope.setAttribute(
          GML.getWord(version.getGmlVersion(), GML.VOCABULARY.SRSNAME),
          env.getEpsgCrs().toUrnString());
    } else {
      envelope.setAttribute(
          GML.getWord(version.getGmlVersion(), GML.VOCABULARY.SRSNAME),
          env.getEpsgCrs().toSimpleString());
    }

    bbox.appendChild(envelope);

    Element lower =
        doc.createElementNS(
            GML.getNS(version.getGmlVersion()),
            GML.getWord(version.getGmlVersion(), GML.VOCABULARY.LOWER_CORNER));
    lower.setTextContent(min);
    envelope.appendChild(lower);

    Element upper =
        doc.createElementNS(
            GML.getNS(version.getGmlVersion()),
            GML.getWord(version.getGmlVersion(), GML.VOCABULARY.UPPER_CORNER));
    upper.setTextContent(max);
    envelope.appendChild(upper);
  }

  @Override
  public Map<String, String> toKVP(VERSION version, XMLNamespaceNormalizer nsStore) {
    String min = env.getXmin() + "," + env.getYmin();
    String max = env.getXmax() + "," + env.getYmax();
    String bbox = min + "," + max;
    bbox += "," + env.getEpsgCrs().toSimpleString();

    return ImmutableMap.of(
        FES.getWord(version, FES.VOCABULARY.BBOX).toUpperCase(Locale.ROOT), bbox);
  }
}
