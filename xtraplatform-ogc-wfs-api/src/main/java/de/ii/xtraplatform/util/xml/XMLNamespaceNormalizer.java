/**
 * Copyright 2017 European Union, interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
/**
 * bla
 */
package de.ii.xtraplatform.util.xml;

import de.ii.xsf.logging.XSFLogger;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import de.ii.xtraplatform.ogc.api.i18n.FrameworkMessages;
import org.forgerock.i18n.slf4j.LocalizedLogger;

/**
 *
 * @author fischer
 */
public class XMLNamespaceNormalizer {

    private static final LocalizedLogger LOGGER = XSFLogger.getLogger(XMLNamespaceNormalizer.class);
    private Map<String, String> namespaces;
    private int nscount;
    private int shortcount;
    private final Map<String, String> shortNamespaces;

    public XMLNamespaceNormalizer() {
        namespaces = new HashMap<>();
        shortNamespaces = new HashMap<>();
        nscount = 0;
        shortcount = 0;
    }

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    public Map<String, String> xgetShortPrefixNamespaces() {
        Map<String, String> shortns = new HashMap<>();

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
            //defaultNamespaceURI = namespaceURI;
            //prefix = defaultNamespacePRE;
            LOGGER.debug(FrameworkMessages.ADDED_DEFAULT_NAMESPACE, prefix, namespaceURI);
            this.addNamespace(namespaceURI);
        }

        if (!namespaces.containsValue(namespaceURI)) {
            // force gml prefix for gml namespace (some WFS want it like that ... [carbon])
            if (namespaceURI.startsWith("http://www.opengis.net/gml")) {
                namespaces.put("gml", namespaceURI);
                LOGGER.debug(FrameworkMessages.ADDED_GML_NAMESPACE, "gml", namespaceURI);
            } else if (!namespaces.containsValue(namespaceURI) && prefix != null) {
                namespaces.put(prefix, namespaceURI);
                LOGGER.debug(FrameworkMessages.ADDED_NAMESPACE, prefix, namespaceURI);
            }
        }

        if (prefix != null && prefix.length() > 5 && !shortNamespaces.containsValue(namespaceURI)) {
            String pre = prefix.substring(0, 5) + shortcount++;
            shortNamespaces.put(pre, namespaceURI);
        }

    }

    public void addNamespace(String namespaceURI) {
        if (!namespaces.containsValue(namespaceURI)) {
            String prefix = "ns" + nscount++;
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
        return qn.substring(qn.lastIndexOf(":") + 1);
    }

    public String getQualifiedName(String lqn) {

        String prefix = this.getNamespacePrefix(extractURI(lqn));
        String ftn = getLocalName(lqn);

        return prefix + ":" + ftn;
    }

    public String generateNamespaceDeclaration(String prefix) {
        return "xmlns:" + prefix + "=\"" + this.getNamespaceURI(prefix) + "\"";
    }

    public String extractURI(String qn) {
        if (qn.contains(":")) {
            return qn.substring(0, qn.lastIndexOf(":"));
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
