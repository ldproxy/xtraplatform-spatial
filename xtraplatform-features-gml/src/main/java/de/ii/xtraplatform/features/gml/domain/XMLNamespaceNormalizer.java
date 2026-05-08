/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.namespace.QName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fischer
 */
public class XMLNamespaceNormalizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(XMLNamespaceNormalizer.class);
  private Map<String, String> namespaces;
  private int nscount;
  private int shortcount;
  private final Map<String, String> shortNamespaces;

  public XMLNamespaceNormalizer() {
    namespaces = new LinkedHashMap<>();
    shortNamespaces = new LinkedHashMap<>();
    nscount = 0;
    shortcount = 0;
  }

  public XMLNamespaceNormalizer(Map<String, String> namespaces) {
    this();
    this.namespaces.putAll(namespaces);
  }

  public Map<String, String> getNamespaces() {
    return namespaces;
  }

  public Map<String, String> xgetShortPrefixNamespaces() {
    Map<String, String> shortns = new LinkedHashMap<>();

    for (Map.Entry<String, String> ns : namespaces.entrySet()) {
      boolean add = true;
      for (Map.Entry<String, String> ns0 : shortNamespaces.entrySet()) {
        if (ns.getValue().equals(ns0.getValue())) {
          shortns.put(ns0.getKey(), ns0.getValue());
          add = false;
        }
      }
      if (add) {
        shortns.put(ns.getKey(), ns.getValue());
      }
    }
    return shortns;
  }

  public Set<String> xgetNamespaceUris() {
    return namespaces.keySet();
  }

  public void setNamespaces(Map<String, String> namespaces) {
    this.namespaces = namespaces;

    for (Map.Entry<String, String> ns : namespaces.entrySet()) {
      if (ns.getKey() != null && ns.getKey().length() > 5) {
        String pre = ns.getKey().substring(0, 5) + shortcount++;
        shortNamespaces.put(pre, ns.getValue());
      }
    }
  }

  public void addNamespace(String prefix, String namespaceURI, boolean overwritePrefix) {
    if (prefix != null && namespaces.containsKey(prefix)) {
      namespaces.remove(prefix);
    }
    addNamespace(prefix, namespaceURI);
  }

  public void addNamespace(String prefix, String namespaceURI) {

    if (namespaces.containsKey(prefix)) {
      prefix += "x";
    }

    if (prefix != null && prefix.isEmpty()) {
      // defaultNamespaceURI = namespaceURI;
      // prefix = defaultNamespacePRE;
      // LOGGER.debug("Added Default-Namespace: {}, {}", prefix, namespaceURI);
      this.addNamespace(namespaceURI);
    }

    if (!namespaces.containsValue(namespaceURI)) {
      // force gml prefix for gml namespace (some WFS want it like that ... [carbon])
      if (namespaceURI.startsWith("http://www.opengis.net/gml")) {
        namespaces.put("gml", namespaceURI);
        // LOGGER.debug("Added gml Namespace: {}, {}", "gml", namespaceURI);
      } else if (!namespaces.containsValue(namespaceURI) && prefix != null) {
        namespaces.put(prefix, namespaceURI);
        // LOGGER.debug("Added Namespace: {}, {}", prefix, namespaceURI);
      }
    }

    if (prefix != null && prefix.length() > 5 && !shortNamespaces.containsValue(namespaceURI)) {
      String pre = prefix.substring(0, 5) + shortcount++;
      shortNamespaces.put(pre, namespaceURI);
    }
  }

  public void addNamespace(String namespaceURI) {
    if (!namespaces.containsValue(namespaceURI)) {
      String prefix;
      Matcher last = Pattern.compile("\\/([a-zA-Z]+)$").matcher(namespaceURI);
      Matcher nextToLast = Pattern.compile("\\/([a-zA-Z]+)\\/[0-9.]+$").matcher(namespaceURI);
      if (last.find()) {
        prefix = last.group(1);
      } else if (nextToLast.find()) {
        prefix = nextToLast.group(1);
      } else {
        prefix = "ns" + nscount++;
      }
      addNamespace(prefix, namespaceURI);
    }
  }

  /*
  public String getDefaultNamespaceURI() {
  return defaultNamespaceURI;
  }

  public String getDefaultNamespacePRE() {
  return defaultNamespacePRE;
  }
  */

  public String convertToShortForm(String longform) {
    for (Map.Entry<String, String> ns : namespaces.entrySet()) {
      if (ns != null && !ns.getValue().isEmpty()) {
        longform = longform.replace(ns.getValue(), this.getNamespacePrefix(ns.getValue()));
      }
    }
    return longform;
  }

  /*
  private String extractNamespaceURI(String qn) {

  int firstIndex = 0;
  if (!qn.contains("http")) { // is this safe? how to tell between the : and the / notation ...
  firstIndex = qn.lastIndexOf("/") + 1;
  }

  int lastIndex = qn.lastIndexOf(":");

  if (lastIndex < 0) {
  return "";
  }

  return qn.substring(firstIndex, lastIndex);
  }
  */
  public String getLocalName(String qn) {
    return qn.substring(qn.lastIndexOf(':') + 1);
  }

  public String getQualifiedName(String lqn) {

    String prefix = this.getNamespacePrefix(extractURI(lqn));
    String ftn = getLocalName(lqn);

    return prefix + ":" + ftn;
  }

  // TODO: change path serialization format
  public String getPrefixedPath(String qualifiedPath) {
    String prefixedPath = qualifiedPath;
    for (Map.Entry<String, String> ns : namespaces.entrySet()) {
      prefixedPath = prefixedPath.replaceAll(ns.getValue(), ns.getKey());
    }
    return prefixedPath;

    /*return Splitter.on('/').splitToList(qualifiedPath).stream()
    .map(this::getQualifiedName)
    .collect(Collectors.joining("/"));*/
  }

  public String generateNamespaceDeclaration(String prefix) {
    return "xmlns:" + prefix + "=\"" + this.getNamespaceURI(prefix) + "\"";
  }

  public String extractURI(String qn) {
    if (qn.contains(":")) {
      return qn.substring(0, qn.lastIndexOf(':'));
    } else {
      return "";
    }
  }

  public String extractPrefix(String qn) {
    return this.extractURI(qn);
  }

  public String getNamespaceURI(String prefix) {
    return namespaces.get(prefix);
  }

  public String getNamespacePrefix(String uri) {
    for (Map.Entry<String, String> ns : namespaces.entrySet()) {
      if (ns.getValue().equals(uri)) {
        return ns.getKey();
      }
    }
    return "";
  }

  public String getShortNamespacePrefix(String uri) {
    for (Map.Entry<String, String> ns : shortNamespaces.entrySet()) {
      if (ns.getValue().equals(uri)) {
        return ns.getKey();
      }
    }
    return getNamespacePrefix(uri);
  }

  public String getQualifiedName(String uri, String localName) {
    return getNamespacePrefix(uri) + ":" + localName;
  }

  public QName getQName(String prefixedName) {
    String prefix = extractPrefix(prefixedName);
    String namespace = getNamespaceURI(prefix);
    String localName = getLocalName(prefixedName);

    return new QName(namespace, localName, prefix);
  }

  public QName getQName(String uri, String localName) {
    String prefix = getNamespacePrefix(uri);

    return new QName(uri, localName, prefix);
  }

  public String getPrefixedName(QName qName) {
    return getQualifiedName(qName.getNamespaceURI(), qName.getLocalPart());
  }

  /*
  public String xgetNamespaceQueryParam(String uri, WFS.VERSION wfsVersion) {
  if (wfsVersion.compareTo(WFS.VERSION._2_0_0) >= 0) {
  return "xmlns(" + getNamespacePrefix(uri) + "," + uri + ")";
  } else {
  return "";
  }
  }


  public String xgetNamespaceQueryAttributeFormat(String uri, WFS.VERSION wfsVersion) {
  if (wfsVersion.compareTo(WFS.VERSION._2_0_0) >= 0) {
  return "xmlns:" + getNamespacePrefix(uri) + "=\"" + uri + "\" ";
  } else {
  return "";
  }
  }*/
}
