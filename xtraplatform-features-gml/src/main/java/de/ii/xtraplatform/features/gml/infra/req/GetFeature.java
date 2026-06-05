/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.req;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocument;
import de.ii.xtraplatform.features.gml.infra.xml.XMLDocumentFactory;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.xml.transform.TransformerException;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * @author zahnen
 */
public class GetFeature implements WfsOperation {
  enum ResultType {
    RESULT,
    HITS
  }

  private final List<WfsQuery> query;
  private final Integer count;
  private final Integer startIndex;
  private final ResultType resultType;
  private final Map<String, String> additionalOperationParameters;

  public GetFeature(
      List<WfsQuery> query,
      Integer count,
      Integer startIndex,
      ResultType resultType,
      Map<String, String> additionalOperationParameters) {
    this.query = query;
    this.count = count;
    this.startIndex = startIndex;
    this.resultType = resultType;
    this.additionalOperationParameters = additionalOperationParameters;
  }

  @Override
  public WFS.OPERATION getOperation() {
    return WFS.OPERATION.GET_FEATURE;
  }

  @Override
  public XMLDocument asXml(XMLDocumentFactory documentFactory, Versions versions)
      throws TransformerException, IOException, SAXException {
    final XMLDocument doc = documentFactory.newDocument();
    doc.addNamespace(WFS.getNS(versions.getWfsVersion()), WFS.getPR(versions.getWfsVersion()));

    Element operation =
        doc.createElementNS(
            WFS.getNS(versions.getWfsVersion()), getOperationName(versions.getWfsVersion()));
    operation.setAttribute("service", "WFS");
    doc.appendChild(operation);

    if (this.count != null) {
      operation.setAttribute(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.COUNT), String.valueOf(count));
    }

    if (this.startIndex != null && versions.getWfsVersion().isGreaterOrEqual(WFS.VERSION._2_0_0)) {
      operation.setAttribute(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.STARTINDEX),
          String.valueOf(startIndex));
    }

    if (this.resultType == ResultType.HITS) {
      operation.setAttribute(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.RESULT_TYPE),
          String.valueOf(ResultType.HITS).toLowerCase(Locale.ROOT));
    }

    if (versions.getGmlVersion() != null && versions.getWfsVersion() != null) {
      operation.setAttribute(
          GML.getWord(versions.getWfsVersion(), WFS.VOCABULARY.OUTPUT_FORMAT),
          GML.getWord(versions.getGmlVersion(), GML.VOCABULARY.OUTPUTFORMAT_VALUE));
      operation.setAttribute(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.VERSION),
          versions.getWfsVersion().toString());
    }

    if (!additionalOperationParameters.isEmpty()) {
      additionalOperationParameters.forEach(operation::setAttribute);
    }

    for (WfsQuery q : query) {
      operation.appendChild(q.asXml(doc, versions));
    }

    doc.done();

    return doc;
  }

  @Override
  public Map<String, String> asKvp(XMLDocumentFactory documentFactory, Versions versions)
      throws TransformerException, IOException, SAXException {
    final XMLDocument doc = documentFactory.newDocument();
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    builder.put("SERVICE", "WFS");
    builder.put("REQUEST", getOperationName(versions.getWfsVersion()));

    // TODO builder.put(GML.getWord(versions.getWfsVersion(),
    // WFS.VOCABULARY.OUTPUT_FORMAT).toUpperCase(), GML.getWord(versions.getGmlVersion(),
    // GML.VOCABULARY.OUTPUTFORMAT_VALUE));
    builder.put(
        WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.VERSION).toUpperCase(Locale.ROOT),
        versions.getWfsVersion().toString());

    if (this.count != null) {
      builder.put(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.COUNT).toUpperCase(Locale.ROOT),
          String.valueOf(count));
    }

    if (this.startIndex != null && versions.getWfsVersion().isGreaterOrEqual(WFS.VERSION._2_0_0)) {
      builder.put(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.STARTINDEX).toUpperCase(Locale.ROOT),
          String.valueOf(startIndex));
    }

    if (this.resultType == ResultType.HITS) {
      builder.put(
          WFS.getWord(versions.getWfsVersion(), WFS.VOCABULARY.RESULT_TYPE)
              .toUpperCase(Locale.ROOT),
          String.valueOf(ResultType.HITS).toLowerCase(Locale.ROOT));
    }

    if (!additionalOperationParameters.isEmpty()) {
      additionalOperationParameters.forEach(builder::put);
    }

    if (!query.isEmpty()) {
      builder.putAll(query.get(0).asKvp(doc, versions));
    }

    final String namespaces =
        doc.getNamespaceNormalizer().getNamespaces().keySet().stream()
            .map(
                prefix ->
                    "xmlns("
                        + prefix
                        + ","
                        + doc.getNamespaceNormalizer().getNamespaceURI(prefix)
                        + ")")
            .collect(Collectors.joining(","));

    builder.put("NAMESPACES", namespaces);

    return builder.build();
  }
}
