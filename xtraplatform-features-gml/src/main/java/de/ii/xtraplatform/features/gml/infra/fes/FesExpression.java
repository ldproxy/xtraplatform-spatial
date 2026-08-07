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
import java.util.Map;
import org.w3c.dom.Element;

public abstract class FesExpression {

  public abstract void appendXml(FES.VERSION version, Element e, XMLDocument doc);

  public Map<String, String> toKVP(FES.VERSION version, XMLNamespaceNormalizer nsStore) {
    return ImmutableMap.of();
  }
}
