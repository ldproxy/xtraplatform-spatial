/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FeaturePropertyTransformerCoalesce
    implements FeaturePropertyTokenSliceTransformer {

  private static final String TYPE = "COALESCE";

  private FeatureSchema schema;

  @Override
  public String getType() {
    return TYPE;
  }

  public abstract boolean isObject();

  public abstract List<List<FeaturePropertyValueTransformer>> valueTransformers();

  @Override
  public FeatureSchema transformSchema(FeatureSchema schema) {
    // checkArray(schema);
    this.schema = schema;

    return schema.with(builder -> builder.type(isObject() ? Type.OBJECT : Type.VALUE));
  }

  @Override
  public List<Object> transform(String currentPropertyPath, List<Object> slice) {
    if (slice.isEmpty() || Objects.isNull(schema)) {
      return slice;
    }

    List<String> rootPath = getRootPath(slice);
    boolean isArray = slice.get(0) == FeatureTokenType.ARRAY;
    List<Object> transformed = new ArrayList<>();

    if (!isArray) {
      return slice;
    }

    if (isObject()) {
      int start = findPos(slice, FeatureTokenType.OBJECT, rootPath, 0);

      if (start > -1) {
        int end = findPos(slice, FeatureTokenType.OBJECT_END, rootPath, start);

        return coalesceObjectProperties(slice, start + 2, end, rootPath);
      }
    } else {
      return coalesceValues(slice);
    }

    return transformed;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private List<Object> coalesceValues(List<Object> slice) {
    List<Object> transformed = new ArrayList<>();
    boolean skip = false;
    boolean found = false;
    int valueIndex = -1;

    for (int i = 0; i < slice.size(); i++) {
      if (isValueWithPath(slice, i, schema.getFullPath())) {
        if (found) {
          skip = true;
        } else {
          skip = true;
          valueIndex++;

          String value = (String) slice.get(i + 2);
          for (FeaturePropertyValueTransformer transformer : valueTransformers().get(valueIndex)) {
            value = transformer.transform(getPropertyPath(), value);
          }
          if (Objects.nonNull(value)) {
            transformed.add(slice.get(i));
            transformed.add(slice.get(i + 1));
            transformed.add(value);
            transformed.add(slice.get(i + 3));

            found = true;
          }
        }
      } else if (isArrayWithPath(slice, i, schema.getFullPath())
          || isArrayEndWithPath(slice, i, schema.getFullPath())) {
        skip = true;
      } else if (skip && slice.get(i) instanceof FeatureTokenType) {
        skip = false;
      }
      if (!skip) {
        transformed.add(slice.get(i));
      }
    }

    return transformed;
  }

  private List<Object> coalesceObjectProperties(
      List<Object> slice, int start, int end, List<String> rootPath) {
    List<Object> transformed = new ArrayList<>();
    boolean found = false;

    Map<String, Integer> valueIndexes = getValueIndexesByProp(slice, start, end, rootPath.size());

    for (int i = 0; i < schema.getCoalesce().size(); i++) {
      String prefix = i + "_";
      if (valueIndexes.keySet().stream().anyMatch(prop -> prop.startsWith(prefix))) {
        valueIndexes.forEach(
            (prop, index) -> {
              if (prop.startsWith(prefix)) {
                transformed.add(slice.get(index - 2));
                transformed.add(slice.get(index - 1));
                transformed.add(slice.get(index));
                transformed.add(slice.get(index + 1));
              }
            });

        // TODO: check if all required props are set, otherwise reset and continue

        found = true;
        break;
      }
    }

    if (found) {
      transformed.add(0, FeatureTokenType.OBJECT);
      transformed.add(1, rootPath);
      transformed.add(FeatureTokenType.OBJECT_END);
      transformed.add(rootPath);
    }

    return transformed;
  }

  @Override
  public void transformObject(
      String currentPropertyPath,
      List<Object> slice,
      List<String> rootPath,
      int start,
      int end,
      List<Object> result) {}
}
