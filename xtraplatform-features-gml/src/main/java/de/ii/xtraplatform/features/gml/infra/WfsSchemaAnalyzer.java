/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.infra;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureProviderSchemaConsumer;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.MappingBuilder;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.gml.domain.GmlGeometryType;
import de.ii.xtraplatform.features.gml.domain.GmlType;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.features.gml.infra.req.GML;
import de.ii.xtraplatform.features.gml.infra.xml.XMLPathTracker;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.xml.namespace.QName;

class WfsSchemaAnalyzer implements FeatureProviderSchemaConsumer {

  public static final String GML_NS_URI = GML.getNS(GML.VERSION._2_1_1);

  private final Set<String> mappedPaths;
  private final XMLNamespaceNormalizer namespaceNormalizer;
  private final Map<QName, String> typeIds;
  private final MappingBuilder mappingBuilder;
  private final XMLPathTracker currentPath;
  private boolean skip;

  WfsSchemaAnalyzer(List<QName> featureTypes, Map<String, String> namespaces) {
    this.currentPath = new XMLPathTracker();
    this.mappedPaths = new HashSet<>();
    this.namespaceNormalizer = new XMLNamespaceNormalizer(namespaces);
    this.typeIds = getTypeIds(featureTypes);
    this.mappingBuilder = new MappingBuilder();
    this.skip = false;
  }

  private Map<QName, String> getTypeIds(List<QName> featureTypes) {
    return featureTypes.stream()
        .map(
            qName -> {
              boolean hasConflict =
                  featureTypes.stream()
                      .anyMatch(
                          qName1 ->
                              Objects.equals(qName.getLocalPart(), qName1.getLocalPart())
                                  && !Objects.equals(qName, qName1));

              String id =
                  hasConflict
                      ? WfsCapabilitiesAnalyzer.getLongFeatureTypeId(
                          namespaceNormalizer.getPrefixedName(qName), namespaceNormalizer)
                      : WfsCapabilitiesAnalyzer.getShortFeatureTypeId(
                          namespaceNormalizer.getPrefixedName(qName), namespaceNormalizer);

              return new SimpleImmutableEntry<>(qName, id);
            })
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public List<FeatureSchema> getFeatureTypes() {
    return mappingBuilder.getFeatureTypes();
  }

  @Override
  public void analyzeNamespace(String uri) {
    namespaceNormalizer.addNamespace(uri);
  }

  @Override
  public void analyzeFeatureType(String nsUri, String localName) {
    String currentPrefixedName = namespaceNormalizer.getQualifiedName(nsUri, localName);
    QName currentQualifiedName = namespaceNormalizer.getQName(nsUri, localName);

    mappedPaths.clear();
    currentPath.clear();

    if (
    /*!mappingBuilder.getFeatureTypes().isEmpty() ||*/ !typeIds.containsKey(currentQualifiedName)) {
      this.skip = true;
      return;
    }

    String currentLocalName = typeIds.get(currentQualifiedName);
    this.skip = false;

    mappingBuilder.openType(currentLocalName, List.of(currentPrefixedName));
  }

  @Override
  public void analyzeAttribute(
      String nsUri, String localName, String type, boolean required, int depth) {
    // only first level gml:ids
    if (skip) {
      return;
    }

    currentPath.track(
        namespaceNormalizer.getNamespacePrefix(nsUri), "@" + localName, depth + 1, false);

    if (depth == 0
        && (("id".equals(localName) && nsUri.startsWith(GML_NS_URI)) || "fid".equals(localName))) {
      String path = currentPath.toString();
      // if (!isPathMapped(path)) {
      mappingBuilder.addValue(
          "id", currentPath.asList(), FeatureSchema.Type.STRING, FeatureSchema.Role.ID);
      // }
    } else if (depth > 0) {
      String path = currentPath.toString();
      if (isPathMapped(path)
          && (Objects.equals(localName, "href") || Objects.equals(localName, "title"))) {
        String fieldNameGml = currentPath.toFieldNameGml().replaceAll("@", "");
        mappingBuilder.addValue(
            getShortPropertyName(fieldNameGml), currentPath.asList(), FeatureSchema.Type.STRING);
      }
    }
  }

  @Override
  public void analyzeProperty(
      String nsUri,
      String localName,
      String type,
      long minOccurs,
      long maxOccurs,
      int depth,
      boolean isParentMultiple,
      boolean isComplex,
      boolean isObject) {
    currentPath.track(namespaceNormalizer.getNamespacePrefix(nsUri), localName, depth, false);
    String path = currentPath.toString();

    // skip first level gml properties
    if ((nsUri.startsWith(GML_NS_URI)
            && (path.startsWith(namespaceNormalizer.getNamespacePrefix(nsUri) + ":")
                || mappingBuilder.getPrev().filter(FeatureSchema::isSpatial).isPresent()))
        || skip) {
      return;
    }

    boolean isMultiple = maxOccurs > 1 || maxOccurs == -1;
    boolean isRequired = minOccurs > 0;

    // if (!isPathMapped(path)) {
    Optional<FeatureSchema.Type> propertyType =
        getPropertyType(
            type, isMultiple, isParentMultiple, isComplex, isObject, currentPath.asList().size());

    // LOGGER.debug("IS {} {} {} {} {}", path, isComplex, isObject, depth, type);

    if (propertyType.isPresent()) {
      String fieldNameGml = currentPath.toFieldNameGml();

      if ("id".equals(fieldNameGml)) {
        fieldNameGml = "_id_";
      }

      mappedPaths.add(path);

      switch (propertyType.get()) {
        case GEOMETRY:
          mappingBuilder.addGeometry(
              getShortPropertyName(fieldNameGml),
              currentPath.asList(),
              GmlGeometryType.fromString(type).toSimpleFeatureGeometry());
          break;
        case OBJECT:
        case OBJECT_ARRAY:
          mappingBuilder.openObject(
              getShortPropertyName(fieldNameGml), currentPath.asList(), propertyType.get());
          break;
        case VALUE_ARRAY:
          mappingBuilder.addValueArray(
              getShortPropertyName(fieldNameGml),
              currentPath.asList(),
              getFeatureSchemaType(GmlType.fromString(type)));
          break;
        default:
          mappingBuilder.addValue(
              getShortPropertyName(fieldNameGml), currentPath.asList(), propertyType.get());
          break;
      }
    }
    // }
  }

  private Optional<Type> getPropertyType(
      String type,
      boolean isMultiple,
      boolean isParentMultiple,
      boolean isComplex,
      boolean isObject,
      int pathLength) {
    boolean isObj =
        ((isComplex || isObject) && pathLength % 2 == 0) || Objects.equals(type, "ReferenceType");

    if ((isMultiple || isParentMultiple) && isObj) {
      return Optional.of(FeatureSchema.Type.OBJECT_ARRAY);
    }
    if (isMultiple && GmlType.fromString(type).isValid()) {
      return Optional.of(FeatureSchema.Type.VALUE_ARRAY);
    }
    if (isObj) {
      return Optional.of(FeatureSchema.Type.OBJECT);
    }
    if (GmlType.fromString(type).isValid()) {
      return Optional.of(getFeatureSchemaType(GmlType.fromString(type)));
    }
    if (GmlGeometryType.fromString(type).isValid()) {
      return Optional.of(FeatureSchema.Type.GEOMETRY);
    }
    return Optional.empty();
  }

  private String getShortPropertyName(String fullName) {
    String[] nameTokens = fullName.replace("[]", "").split("\\.");
    return nameTokens[nameTokens.length - 1];
  }

  @Override
  public boolean analyzeNamespaceRewrite(
      String oldNamespace, String newNamespace, String featureTypeName) {
    return false;
  }

  @Override
  public void analyzeFailure(Throwable e) {}

  @Override
  public void analyzeSuccess() {
    // finish last feature type
    /*if (Objects.nonNull(currentFeatureType) && Objects.nonNull(currentLocalName)) {
      addLastProperty();
      featureTypes.add(currentFeatureType.build());
    }*/
  }

  private FeatureSchema.Type getFeatureSchemaType(GmlType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return FeatureSchema.Type.BOOLEAN;
      case STRING:
      case URI:
        return FeatureSchema.Type.STRING;
      case INT:
      case INTEGER:
      case SHORT:
      case LONG:
        return FeatureSchema.Type.INTEGER;
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return FeatureSchema.Type.FLOAT;
      case DATE:
      case DATE_TIME:
        return FeatureSchema.Type.DATETIME;
      default:
        return FeatureSchema.Type.UNKNOWN;
    }
  }

  // this prevents that we descend further on a mapped path
  private boolean isPathMapped(String path) {
    if (!path.contains("/")) {
      return false;
    }

    String parent = path.substring(0, path.lastIndexOf('/'));
    for (String mappedPath : mappedPaths) {
      if (parent.equals(mappedPath)) {
        return true;
      }
    }
    return false;
  }
}
