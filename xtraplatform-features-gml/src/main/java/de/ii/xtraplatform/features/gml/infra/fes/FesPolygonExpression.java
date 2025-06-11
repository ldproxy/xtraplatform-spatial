/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.fes;

import de.ii.xtraplatform.features.gml.infra.req.FES;
import de.ii.xtraplatform.features.gml.infra.req.GML;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.util.List;
import java.util.Map;
import org.w3c.dom.Element;

public class FesPolygonExpression extends FesExpression {

  private Polygon poly;
  private String geometryPath;

  public FesPolygonExpression(Polygon env, String geometryPath) {
    this.geometryPath = geometryPath;
    this.poly = env;
  }

  @Override
  public void toXML(FES.VERSION version, Element e, XMLDocument doc) {

    doc.addNamespace(FES.getNS(version), FES.getPR(version));
    Element intersects =
        doc.createElementNS(FES.getNS(version), FES.getWord(version, FES.VOCABULARY.INTERSECTS));
    e.appendChild(intersects);

    Element valRef =
        doc.createElementNS(
            FES.getNS(version), FES.getWord(version, FES.VOCABULARY.VALUE_REFERENCE));
    intersects.appendChild(valRef);
    valRef.setTextContent(geometryPath);

    // Element literal = doc.createElementNS(FES.getNS(version), FES.getPR(version),
    // FES.getWord(version, FES.VOCABULARY.LITERAL));
    // intersects.appendChild(literal);

    GML.VERSION gmlversion = version.getGmlVersion();
    doc.addNamespace(GML.getNS(gmlversion), GML.getPR(gmlversion));
    Element polygon =
        doc.createElementNS(GML.getNS(gmlversion), GML.getWord(gmlversion, GML.VOCABULARY.POLYGON));
    intersects.appendChild(polygon);

    if (version.isGreaterOrEqual(FES.VERSION._2_0_0)) {
      polygon.setAttribute(
          GML.getWord(version.getGmlVersion(), GML.VOCABULARY.SRSNAME),
          poly.getCrs().toUrnString());
    } else {
      polygon.setAttribute(
          GML.getWord(version.getGmlVersion(), GML.VOCABULARY.SRSNAME),
          poly.getCrs().toSimpleString());
    }
    polygon.setAttribute(GML.getWord(version.getGmlVersion(), GML.VOCABULARY.SRSDIMENSION), "2");

    polygon.setAttribute(GML.getWord(version.getGmlVersion(), GML.VOCABULARY.GMLID), "Id1");

    for (List<List<Double>> rings : poly.getRings()) {

      // TODO: interior ...
      Element exterior =
          doc.createElementNS(
              GML.getNS(gmlversion), GML.getWord(gmlversion, GML.VOCABULARY.EXTERIOR));
      polygon.appendChild(exterior);

      Element linearring =
          doc.createElementNS(
              GML.getNS(gmlversion), GML.getWord(gmlversion, GML.VOCABULARY.LINEAR_RING));
      exterior.appendChild(linearring);

      StringBuilder c = new StringBuilder();
      for (List<Double> ring : rings) {
        for (Double d : ring) {
          c.append(d);
          c.append(" ");
        }
      }

      Element poslist =
          doc.createElementNS(
              GML.getNS(gmlversion), GML.getWord(gmlversion, GML.VOCABULARY.POS_LIST));
      linearring.appendChild(poslist);
      poslist.setTextContent(c.toString());
    }
  }

  public void toKVP(FES.VERSION version, Map<String, String> params) {

    /*
    String min = env.getXmin() + "," + env.getYmin();
    String max = env.getXmax() + "," + env.getYmax();
    String bbox = min+","+max;

    if( !env.getSpatialReference().getWkidString().equals("4326")) {
    bbox += ","+env.getSpatialReference().getEPSGCode();
    }
    params.put(FES.getWord(version, FES.VOCABULARY.BBOX).toUpperCase(), bbox);
    */
  }
}
