/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FeatureSchemaToTypeVisitor implements SchemaVisitor<FeatureSchema, FeatureType> {

  private final String featureTypeName;

  public FeatureSchemaToTypeVisitor(String featureTypeName) {
    this.featureTypeName = featureTypeName;
  }

  @Override
  public FeatureType visit(FeatureSchema schema, List<FeatureType> visitedProperties) {

    ImmutableFeatureType.Builder builder =
        new ImmutableFeatureType.Builder().name(schema.getName());

    if (schema.isValue()) {
      String name = getName(schema);
      return builder
          .putProperties(
              name,
              new ImmutableFeatureProperty.Builder()
                  .name(name)
                  .path(getPath(schema))
                  .type(
                      FeatureProperty.Type.valueOf(
                          schema
                              .getValueType()
                              .map(SchemaBase.Type::name)
                              .orElse(schema.getType().name())))
                  .role(schema.getRole().map(role -> FeatureProperty.Role.valueOf(role.name())))
                  .build())
          .build();
    }

    Map<String, FeatureProperty> properties =
        visitedProperties.stream()
            .flatMap(
                types -> {
                  return types.getProperties().values().stream()
                      .map(
                          property -> {
                            String name =
                                Objects.equals(schema.getName(), featureTypeName)
                                    ? property.getName()
                                    : getName(schema, property.getName());
                            return new ImmutableFeatureProperty.Builder()
                                .from(property)
                                .name(name)
                                .path(getPath(schema, property.getPath()))
                                .constantValue(property.getConstantValue())
                                .additionalInfo(
                                    getAdditionalInfo(
                                        schema, property.getName(), property.getAdditionalInfo()))
                                .build();
                          })
                      .map(
                          property ->
                              new AbstractMap.SimpleImmutableEntry<>(property.getName(), property));
                })
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

    return builder.properties(properties).build();
  }

  private Map<String, ? extends String> getAdditionalInfo(
      FeatureSchema schema, String name, Map<String, String> additionalInfo) {
    if (!additionalInfo.isEmpty()) {
      return additionalInfo;
    }

    // TODO: no longer needed
    if (schema.getObjectType().isPresent()
        && Objects.equals(schema.getObjectType().get(), "Link")) {
      if (Objects.equals(name, "href")) {
        return ImmutableMap.of("role", "LINKHREF");
      }
      if (Objects.equals(name, "title")) {
        return ImmutableMap.of("role", "LINKTITLE");
      }
    }

    return ImmutableMap.of();
  }

  private String getName(FeatureSchema schema) {
    return schema.getName() + (schema.isArray() ? "[]" : "");
  }

  private String getName(FeatureSchema schema, String suffix) {
    return getName(schema) + "." + suffix;
  }

  private String getPath(FeatureSchema schema) {
    return schema.getSourcePath().orElse("");
  }

  private String getPath(FeatureSchema schema, String suffix) {
    String path = getPath(schema);
    return path.isEmpty() ? suffix : path + "/" + suffix;
  }
}
