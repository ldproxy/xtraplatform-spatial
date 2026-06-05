/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.gml.infra.fes.FesFilter;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.xml.transform.TransformerException;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class WfsQuery {

  private final List<String> typeNames;
  private final Optional<FesFilter> filter;
  private final EpsgCrs crs;

  WfsQuery(List<String> typeNames, Optional<FesFilter> filter, EpsgCrs crs) {
    this.typeNames = typeNames;
    this.filter = filter;
    this.crs = crs;
  }

  public Element asXml(XMLDocument document, Versions versions)
      throws IOException, TransformerException, SAXException {
    document.addNamespace(WFS.getNS(versions.getWfsVersion()), WFS.getPR(versions.getWfsVersion()));

    final Element query =
        document.createElementNS(
            WFS.getNS(versions.getWfsVersion()),
            WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.QUERY));

    if (filter.isPresent()) {
      // final Node node =
      // document.adoptDocument(FILTER_ENCODERS.get(versions.getWfsVersion()).encode(filter.get(0)));
      // query.appendChild(node);
      filter.get().appendXml(versions.getWfsVersion().getFilterVersion(), query, document);
    }

    query.setAttribute(
        WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.TYPENAMES), getTypeNames());

    if (this.crs != null) {
      query.setAttribute(WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.SRSNAME), getCrs());
    }

    return query;
  }

  public Map<String, String> asKvp(XMLDocument document, Versions versions)
      throws IOException, TransformerException, SAXException {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put(
        WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.TYPENAMES).toUpperCase(Locale.ROOT),
        getTypeNames());

    if (getCrs() != null) {
      builder.put(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.SRSNAME).toUpperCase(Locale.ROOT),
          getCrs());
    }

    if (filter.isPresent()) {
      builder.putAll(
          filter
              .get()
              .toKVP(
                  versions.getWfsVersion().getFilterVersion(), document.getNamespaceNormalizer()));
    }

    return builder.build();
  }

  private String getTypeNames() {
    return Joiner.on(',').skipNulls().join(typeNames);
  }

  private String getCrs() {
    if (this.crs != null) {
      return crs.toSimpleString();
    }
    return null;
  }
}
