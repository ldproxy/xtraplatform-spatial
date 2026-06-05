/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra.xml;

import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import java.io.StringWriter;
import java.io.Writer;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * @author fischer
 */
public class XMLDocument {

  private static final Logger LOGGER = LoggerFactory.getLogger(XMLDocument.class);
  final Document doc;
  final XMLNamespaceNormalizer nsn;

  XMLDocument(XMLNamespaceNormalizer nsn, Document doc) {
    this.nsn = nsn;
    this.doc = doc;
  }

  public void appendChild(Node element) {
    this.doc.appendChild(element);
  }

  public Node adoptDocument(XMLDocument document) {
    return adoptDocument(document.doc);
  }

  public Node adoptDocument(Node document) {
    Node node = this.doc.adoptNode(document);
    int num = node.getAttributes().getLength() - 1;
    for (int i = num; i >= 0; i--) {
      Attr attr = (Attr) node.getAttributes().item(i);
      if (attr.getName().startsWith("xmlns:")) {
        this.addNamespace(attr.getValue(), attr.getLocalName());
        node.getAttributes().removeNamedItem(attr.getName());
      }
    }

    return node;
  }

  public void addNamespace(String namespace, String prefix) {
    this.nsn.addNamespace(prefix, namespace);
  }

  public Element createElementNS(String namespace, String localName) {

    return this.doc.createElementNS(namespace, this.nsn.getQualifiedName(namespace, localName));
  }

  public void done() {
    for (String uri : nsn.xgetNamespaceUris()) {
      this.doc.getDocumentElement().setAttribute("xmlns:" + uri, nsn.getNamespaceURI(uri));
    }
  }

  public String toString(boolean pretty) {
    return toString(pretty, false);
  }

  // returns the String representation of the XML Document
  public String toString(boolean pretty, boolean omitXmlDeclaration) {
    try {
      Transformer tf = TransformerFactory.newInstance().newTransformer();
      tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      if (pretty) {
        tf.setOutputProperty(OutputKeys.INDENT, "yes");
      }
      if (omitXmlDeclaration) {
        tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      }
      Writer out = new StringWriter();
      tf.transform(new DOMSource(doc), new StreamResult(out));
      return out.toString();
    } catch (Exception ex) {
      LOGGER.error("Error serializing XMLDocument", ex);
    }
    return null;
  }

  public XMLNamespaceNormalizer getNamespaceNormalizer() {
    return nsn;
  }
}
