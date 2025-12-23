/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.base.Joiner;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FeaturePropertyTransformerFlatten
    implements FeaturePropertyTokenSliceTransformer, DynamicTargetSchemaTransformer {

  public enum INCLUDE {
    ALL,
    OBJECTS,
    ARRAYS
  }

  public static final String TYPE = "FLATTEN";

  @Override
  public String getType() {
    return TYPE;
  }

  @Value.Default
  public INCLUDE include() {
    return INCLUDE.ALL;
  }

  @Value.Derived
  public FeatureSchemaFlattener getFlattener() {
    return new FeatureSchemaFlattener(getParameter());
  }

  @Override
  public List<Object> transform(String currentPropertyPath, List<Object> slice) {
    List<Object> transformed = new ArrayList<>();
    boolean isValue = false;
    boolean isGeometry = false;
    int contextIndex = 0;
    boolean isArray = false;
    boolean isArrayEnd = false;
    boolean inArray = false;
    boolean isObject = false;
    Map<List<String>, Integer> arrays = new HashMap<>();
    List<String> arrayPath = null;
    List<String> currentPath = null;

    for (Object token : slice) {
      if (token instanceof FeatureTokenType) {
        isValue = Objects.equals(token, FeatureTokenType.VALUE);
        isGeometry = Objects.equals(token, FeatureTokenType.GEOMETRY);
        contextIndex = 0;
        isArray = Objects.equals(token, FeatureTokenType.ARRAY);
        isArrayEnd = Objects.equals(token, FeatureTokenType.ARRAY_END);
        isObject = Objects.equals(token, FeatureTokenType.OBJECT);
      }

      if (contextIndex == 1 && token instanceof List) {
        currentPath = (List<String>) token;

        if (isArray) {
          arrayPath = currentPath;
          arrays.put(arrayPath, 0);
          inArray = true;
        } else if (isArrayEnd) {
          arrayPath = null;
          inArray = false;
        }

        if (inArray && (isObject || isValue) && currentPath.size() == arrayPath.size()) {
          arrays.computeIfPresent(arrayPath, (ignore, index) -> index + 1);
        }

        if (isValue && inArray) {
          List<String> newPath = new ArrayList<>(arrayPath);
          newPath.set(
              newPath.size() - 1,
              newPath.get(newPath.size() - 1) + "[" + arrays.get(arrayPath) + "]");
          if (currentPath.size() > arrayPath.size()) {
            newPath.add(currentPath.get(currentPath.size() - 1));
          }
          currentPath = newPath;
        }
      }

      if (isValue || isGeometry) {
        if (contextIndex == 1 && token instanceof List) {
          transformed.add(currentPath);
        } else {
          transformed.add(token);
        }
      }

      contextIndex++;
    }

    return transformed;
  }

  @Override
  public FeatureSchema transformSchema(FeatureSchema schema) {
    if (!schema.isFeature()) {
      return schema;
    }

    return schema.accept(getFlattener());
  }

  @Override
  public void transformObject(
      String currentPropertyPath,
      List<Object> slice,
      List<String> rootPath,
      int start,
      int end,
      List<Object> result) {}

  @Override
  public boolean isApplicableDynamic(List<String> path) {
    return path.stream().anyMatch(elem -> elem.matches(".*\\[[0-9]+\\]$"));
  }

  @Override
  public List<String> transformPathDynamic(List<String> path) {
    return path.stream()
        .map(
            elem -> {
              if (elem.endsWith("]")) {
                return elem.substring(0, elem.indexOf("["));
              }
              return elem;
            })
        .collect(Collectors.toList());
  }

  @Override
  public List<FeatureSchema> transformSchemaDynamic(
      List<FeatureSchema> schemas, List<String> path) {
    String name =
        Joiner.on(getParameter())
            .join(MappingOperationResolver.cleanConcatPath(path))
            .replaceAll("\\[", getParameter())
            .replaceAll("\\]", "");

    return schemas.stream()
        .map(schema -> new ImmutableFeatureSchema.Builder().from(schema).name(name).build())
        .collect(Collectors.toList());
  }
}
