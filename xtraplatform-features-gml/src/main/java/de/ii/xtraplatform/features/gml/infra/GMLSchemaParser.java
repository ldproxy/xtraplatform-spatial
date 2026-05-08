/*
 * Copyright 2017 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra;

import com.sun.xml.xsom.XSAttributeUse;
import com.sun.xml.xsom.XSComplexType;
import com.sun.xml.xsom.XSElementDecl;
import com.sun.xml.xsom.XSModelGroup;
import com.sun.xml.xsom.XSParticle;
import com.sun.xml.xsom.XSSchema;
import com.sun.xml.xsom.XSSchemaSet;
import com.sun.xml.xsom.XSTerm;
import com.sun.xml.xsom.XSType;
import com.sun.xml.xsom.parser.XSOMParser;
import de.ii.xtraplatform.features.domain.FeatureProviderSchemaConsumer;
import de.ii.xtraplatform.features.gml.infra.req.GML;
import de.ii.xtraplatform.features.gml.infra.xml.XMLPathTracker;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * @author zahnen
 */
public class GMLSchemaParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(GMLSchemaParser.class);
  private final List<FeatureProviderSchemaConsumer> analyzers;
  private final XMLPathTracker currentPath;
  private final List<XSElementDecl> abstractObjectDecl;
  private final List<XSElementDecl> abstractFeatureDecl;
  private final Map<String, String> namespaces;
  private XSType gcoObjectType;
  private Set<String> complexTypes;
  private final URI baseURI;
  private EntityResolver entityResolver;

  public GMLSchemaParser(
      List<FeatureProviderSchemaConsumer> analyzers, URI baseURI, Map<String, String> namespaces) {
    this(analyzers, baseURI, namespaces, new OGCEntityResolver());
  }

  public GMLSchemaParser(
      List<FeatureProviderSchemaConsumer> analyzers,
      URI baseURI,
      Map<String, String> namespaces,
      EntityResolver entityResolver) {
    this.analyzers = analyzers;
    this.currentPath = new XMLPathTracker();
    this.abstractObjectDecl = new ArrayList<XSElementDecl>();
    this.abstractFeatureDecl = new ArrayList<XSElementDecl>();
    this.baseURI = baseURI;
    this.namespaces = namespaces;
    this.entityResolver = entityResolver;
  }

  public void parse(
      InputStream inputStream,
      Map<String, List<String>> elements,
      Consumer<Map<String, List<String>>> tracker) {
    try {
      InputSource is = new InputSource(inputStream);
      parse(is, elements, tracker);

      for (FeatureProviderSchemaConsumer analyzer : analyzers) {
        analyzer.analyzeSuccess();
      }

    } catch (Exception ex) {
      LOGGER.error("Error parsing application schema. {}", ex);

      for (FeatureProviderSchemaConsumer analyzer : analyzers) {
        analyzer.analyzeFailure(ex);
      }
    }
  }

  private void parse(
      InputSource is,
      Map<String, List<String>> elements,
      Consumer<Map<String, List<String>>> tracker) {
    parse(is, elements, true, tracker);
  }

  private void parse(
      InputSource is,
      Map<String, List<String>> elements,
      boolean lax,
      Consumer<Map<String, List<String>>> tracker) {
    // LOGGER.debug("Parsing GML application schema");
    XSOMParser parser = new XSOMParser();
    Map<String, List<String>> progress = new LinkedHashMap<>();

    try {
      parser.setErrorHandler(new GMLSchemaParserErrorHandler());

      parser.setEntityResolver(entityResolver);

      is.setSystemId(baseURI.toString());

      parser.parse(is);

      XSSchemaSet schemas = parser.getResult();

      schemas
          .getSchemas()
          .forEach(
              xsSchema ->
                  analyzers.forEach(
                      analyzer -> analyzer.analyzeNamespace(xsSchema.getTargetNamespace())));

      for (GML.VERSION version : GML.VERSION.values()) {
        XSSchema schema0 = schemas.getSchema(GML.getWord(version, GML.NAMESPACE.URI));
        if (schema0 != null) {
          XSElementDecl ao =
              schema0.getElementDecl(GML.getWord(version, GML.VOCABULARY.ABSTRACT_OBJECT));
          if (ao != null) {
            abstractObjectDecl.add(ao);
          }
          XSElementDecl af =
              schema0.getElementDecl(GML.getWord(version, GML.VOCABULARY.ABSTRACT_FEATURE));
          if (af != null) {
            abstractFeatureDecl.add(af);
          }
        }
      }

      XSSchema schema1 = schemas.getSchema("http://www.isotc211.org/2005/gco");
      if (schema1 != null) {
        gcoObjectType = schema1.getElementDecl("AbstractObject").getType();
      }

      for (Map.Entry<String, List<String>> ns : elements.entrySet()) {

        String nsuri = ns.getKey();
        String oldNsUri = null;
        // LOGGER.debug("namespace {}", nsuri);

        XSSchema schema = schemas.getSchema(nsuri);

        // workaround for broken WFSs where FeatureTypes are in different namespaces in Capabilities
        // and Schema
        // in this case we search in the targetNamespace of the Schema
        if (schema == null && lax) {
          LOGGER.info(
              "Schema for Namespace '{}' not found, searching in targetNamespace schema instead. ",
              ns.getKey());

          // looks as if the schema for the targetNamespace of the document is always second in the
          // list
          schema = schemas.getSchema(1);
          oldNsUri = nsuri;
          nsuri = schema.getTargetNamespace();
        }

        for (String e : ns.getValue()) {
          track(tracker, e, nsuri, progress);

          XSElementDecl elem = schema.getElementDecl(e);
          if (elem != null && elem.getType().isComplexType()) {
            // LOGGER.debug(" - element {}, type: {}", elem.getName(), elem.getType().getName());

            for (FeatureProviderSchemaConsumer analyzer : analyzers) {
              if (oldNsUri != null) {
                analyzer.analyzeNamespaceRewrite(oldNsUri, nsuri, elem.getName());
              }
              analyzer.analyzeFeatureType(nsuri, elem.getName());
              for (XSAttributeUse att : elem.getType().asComplexType().getAttributeUses()) {
                // LOGGER.debug("   - attribute {}, required: {}, type: {}, ns: {}",
                // att.getDecl().getName(), att.isRequired(), att.getDecl().getType().getName(),
                // att.getDecl().getTargetNamespace());

                analyzer.analyzeAttribute(
                    att.getDecl().getTargetNamespace(),
                    att.getDecl().getName(),
                    att.getDecl().getType().getName(),
                    att.isRequired(),
                    0);
              }
            }
            XSParticle particle = elem.getType().asComplexType().getContentType().asParticle();
            if (particle != null) {
              XSTerm term = particle.getTerm();
              if (term.isModelGroup()) {
                complexTypes = new HashSet<String>();
                parseGroup(term.asModelGroup(), 1, false, i -> {});
              }
            }
          }
        }
      }
    } catch (SAXException ex) {

      // File included in schema not found
      if (ex.getCause() != null
          && ex.getCause().getClass().getName().contains("FileNotFoundException")) {
        throw new IllegalStateException(
            String.format(
                "The GML application schema provided by the WFS imports schema '%s', but that schema cannot be accessed. A valid GML application schema is required to determine the layers of the proxy service and its characteristics.. Please contact the WFS provider to correct the schema error.",
                ex.getCause().getMessage()));
      }

      String msg = ex.toString();
      String msgex = "";
      if (msg != null && !msg.isEmpty()) {
        msg = "Parser details: " + msg;

        msgex = msg.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
      }

      throw new IllegalStateException(
          "The GML application schema provided by the WFS is invalid. A valid GML application schema is required to determine the layers of the proxy service and its characteristics. Please contact the WFS provider to correct the schema error. \n"
              + msgex);
    }
  }

  private void track(
      Consumer<Map<String, List<String>>> tracker,
      String type,
      String nsuri,
      Map<String, List<String>> progress) {
    String nsPrefix =
        namespaces.entrySet().stream()
            .filter(entry -> Objects.equals(entry.getValue(), nsuri))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(nsuri);
    progress.putIfAbsent(nsPrefix, new ArrayList<>());
    progress.get(nsPrefix).add(type);
    tracker.accept(progress);
  }

  private void parseGroup(
      XSModelGroup xsModelGroup, int depth, boolean isParentMultiple, Consumer<Integer> onFeature) {
    // LOGGER.debug(new String(new char[depth]).replace("\0", "  ") + " - {}",
    // xsModelGroup.getCompositor());

    // reset already visited complex types for every first level property of the featuretype,
    // meaning for every new path that should not contain recursion
    if (depth == 2) {
      complexTypes = new HashSet<String>();
    }

    XSParticle[] particles = xsModelGroup.getChildren();

    if (particles.length > 0) {
      for (XSParticle p : particles) {
        XSTerm pterm = p.getTerm();

        if (pterm.isElementDecl()) {
          parseElementDecl(
              pterm.asElementDecl(),
              p.getMinOccurs().longValue(),
              p.getMaxOccurs().longValue(),
              depth,
              isParentMultiple,
              onFeature);
        } else if (pterm.isModelGroup()) {
          parseGroup(pterm.asModelGroup(), depth, false, onFeature);
        } else if (pterm.isModelGroupDecl()) {
          parseGroup(pterm.asModelGroupDecl().getModelGroup(), depth, false, onFeature);
        }
      }
    }
  }

  private void parseElementDecl(
      XSElementDecl decl,
      long minOccurs,
      long maxOccurs,
      int depth,
      boolean isParentMultible,
      Consumer<Integer> onFeature) {

    String propertyName = null;
    XSType propertyType = null;
    boolean isObject = false;
    boolean isFeature = false;

    propertyName = decl.getName();

    String typeName = decl.getType().getBaseType().getName();
    String typeName0 = decl.getType().getName();

    propertyType = decl.getType();

    if (decl.getType().getName() != null) {
      XSElementDecl decl0 = decl.getSubstAffiliation();

      while (decl0 != null) {
        for (XSElementDecl e : abstractFeatureDecl) {
          if (decl0.equals(e)) {
            isFeature = true;
            break;
          }
        }
        if (isFeature) {
          break;
        }

        for (XSElementDecl e : abstractObjectDecl) {
          if (decl0.equals(e)) {
            isObject = true;
            break;
          }
        }
        if (isObject) {
          break;
        }

        decl0 = decl0.getSubstAffiliation();
      }

      if (gcoObjectType != null) {
        if (propertyType.isDerivedFrom(gcoObjectType)) {
          isObject = true;
        }
      }

    } else if (!"anyType".equals(decl.getType().getBaseType().getName())) {
      propertyType = decl.getType().getBaseType();
    }

    if (isFeature && depth > 0) {
      // LOGGER.debug("AF {} {}", propertyName, propertyType);
      onFeature.accept(depth);
      return;
    }

    String propertyTypeName = propertyType.getName();

    if (propertyType.isSimpleType()) {
      if (propertyType.asSimpleType().getPrimitiveType() != null) {
        propertyTypeName =
            propertyType.asSimpleType().getName().matches("int|long")
                ? "int"
                : propertyType.asSimpleType().getPrimitiveType().getName();
      } else if (propertyType.asSimpleType().asUnion() != null) {
        propertyTypeName = "string";
      }
    }

    boolean isComplex = propertyType.isComplexType();
    if (propertyType.isComplexType()
        && propertyType.asComplexType().getContentType().asSimpleType() != null) {
      if (propertyType.asComplexType().getContentType().asSimpleType().getPrimitiveType() != null) {
        propertyType =
            propertyType.asComplexType().getContentType().asSimpleType().getPrimitiveType();
        propertyTypeName = propertyType.getName();
        isComplex = propertyType.isComplexType();
      } else if (propertyType.asComplexType().getContentType().asSimpleType().asUnion() != null) {
        propertyTypeName = "string";
        isComplex = false;
      }
    }

    boolean isMultiple = maxOccurs > 1 || maxOccurs == -1;

    /*if (propertyType.isComplexType() && (maxOccurs > 1 || maxOccurs == -1)) {
      isParentMultible = true;
    }*/

    /*
    LOGGER.debug(new String(new char[depth]).replace("\0", "  ") + " - property, name: {}, type: {}, min: {}, max: {}, ns: {}",
    propertyName, propertyType.getName(), p.getMinOccurs(), p.getMaxOccurs(),
    decl.getTargetNamespace());
    * */
    propertyTypeName = propertyTypeName == null ? "" : propertyTypeName;

    for (FeatureProviderSchemaConsumer analyzer : analyzers) {

      analyzer.analyzeProperty(
          decl.getTargetNamespace(),
          propertyName,
          propertyTypeName,
          minOccurs,
          maxOccurs,
          depth,
          isParentMultible,
          isComplex,
          isObject);

      if (isComplex && Objects.equals(propertyTypeName, "ReferenceType")) {
        for (XSAttributeUse att : propertyType.asComplexType().getAttributeUses()) {
          // LOGGER.debug("   - attribute {}, required: {}, type: {}, ns: {}",
          // att.getDecl().getName(), att.isRequired(), att.getDecl().getType().getName(),
          // att.getDecl().getTargetNamespace());

          analyzer.analyzeAttribute(
              att.getDecl().getTargetNamespace(),
              att.getDecl().getName(),
              att.getDecl().getType().getName(),
              att.isRequired(),
              depth);
        }
      }
    }

    if (propertyType.isComplexType()) {
      XSComplexType type = propertyType.asComplexType();
      String finalPropertyName = propertyName;
      Consumer<Integer> onFeature2 =
          !type.getAttGroups().isEmpty()
                  && Objects.equals(
                      type.getAttGroups().iterator().next().getName(), "AssociationAttributeGroup")
              ? nextDepth -> {
                if (depth == nextDepth - 1) {
                  for (FeatureProviderSchemaConsumer analyzer : analyzers) {
                    analyzer.analyzeProperty(
                        decl.getTargetNamespace(),
                        finalPropertyName,
                        "ReferenceType",
                        minOccurs,
                        maxOccurs,
                        depth,
                        isParentMultible,
                        true,
                        true);

                    analyzer.analyzeAttribute(
                        "http://www.w3.org/1999/xlink", "href", "", false, depth);
                  }
                }
                // LOGGER.debug("ADD HREF {} {}->{}", type.getTargetNamespace() + ":" +
                // type.getName(), depth, nextDepth)
              }
              : nextDepth -> {};

      parseComplexType(type, depth + 1, isMultiple, onFeature2);
    }
  }

  private void parseComplexType(
      XSComplexType type, int depth, boolean isParenMultiple, Consumer<Integer> onFeature) {
    if (type != null) {
      /*for (XSAttributeUse att : elem.getAttributeUses()) {
      LOGGER.debug("   - attribute {}, required: {}, type: {}, ns: {}", att.getDecl().getName(), att.isRequired(), att.getDecl().getType().getName(), att.getDecl().getTargetNamespace());
      analyzer.analyzeAttribute(att.getDecl().getTargetNamespace(), att.getDecl().getName(), att.getDecl().getType().getName(), att.isRequired());
      }*/

      // stop analyzing if the complex type already occurred in this path to avoid recursion
      if (complexTypes.add(type.getTargetNamespace() + ":" + type.getName())) {
        XSParticle particle = type.getContentType().asParticle();
        if (particle != null) {
          XSTerm term = particle.getTerm();
          if (term.isModelGroup()) {
            parseGroup(term.asModelGroup(), depth, isParenMultiple, onFeature);
          }
        }
      }
    }
  }

  private class GMLSchemaParserErrorHandler implements ErrorHandler {

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      throw exception;
    }
  }
}
