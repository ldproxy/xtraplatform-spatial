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
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
public abstract class FeaturePropertyTransformerWrap
    implements FeaturePropertyTokenSliceTransformer {

  private static final String TYPE = "STRUCTURE_WRAP";

  private FeatureSchema schema;

  @Override
  public String getType() {
    return TYPE;
  }

  public abstract Type wrapper();

  @Override
  public FeatureSchema transformSchema(FeatureSchema schema) {
    this.schema = schema;

    return schema;
  }

  @Override
  public List<Object> transform(String currentPropertyPath, List<Object> slice) {
    if (Objects.isNull(schema)) {
      return slice;
    }

    if (wrapper() == Type.VALUE_ARRAY) {
      if (findPos(slice, FeatureTokenType.ARRAY, schema.getFullPath(), 0) > -1) {
        return slice;
      }

      return wrapWithValueArray(slice);
    } else if (wrapper() == Type.OBJECT_ARRAY) {
      if (findPos(slice, FeatureTokenType.ARRAY, schema.getFullPath(), 0) > -1) {
        return slice;
      }

      List<Object> slice2 = slice;
      if (findPos(slice, FeatureTokenType.OBJECT, schema.getFullPath(), 0) == -1) {
        slice2 = wrapSingleValuesWithObject(slice, false);
      }

      return wrapWithObjectArray(slice2);
    } else if (wrapper() == Type.OBJECT) {
      if (findPos(slice, FeatureTokenType.OBJECT, schema.getFullPath(), 0) > -1) {
        return slice;
      }

      return wrapWithObject(slice);
    }

    return slice;
  }

  private List<Object> wrapWithValueArray(List<Object> slice) {
    List<Object> transformed = new ArrayList<>();
    boolean lastWasValueWithPath = false;

    for (int i = 0; i < slice.size(); i++) {
      if (isValueWithPath(slice, i, schema.getFullPath())) {
        if (!lastWasValueWithPath) {
          transformed.add(FeatureTokenType.ARRAY);
          transformed.add(schema.getFullPath());
          lastWasValueWithPath = true;
        }
      } else if (slice.get(i) instanceof FeatureTokenType && lastWasValueWithPath) {
        transformed.add(FeatureTokenType.ARRAY_END);
        transformed.add(schema.getFullPath());
        lastWasValueWithPath = false;
      }

      transformed.add(slice.get(i));

      if (i == slice.size() - 1 && lastWasValueWithPath) {
        transformed.add(FeatureTokenType.ARRAY_END);
        transformed.add(schema.getFullPath());
      }
    }

    return transformed;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private List<Object> wrapWithObjectArray(List<Object> slice) {
    List<Object> transformed = new ArrayList<>();
    boolean foundFirstObject = false;
    boolean objectIsOpen = false;
    boolean arrayIsOpen = false;
    boolean lastWasObjectEnd = false;

    for (int i = 0; i < slice.size(); i++) {
      if (isObjectWithPath(slice, i, schema.getFullPath())) {
        if (!foundFirstObject) {
          transformed.add(FeatureTokenType.ARRAY);
          transformed.add(schema.getFullPath());
          foundFirstObject = true;
          arrayIsOpen = true;
        }
        objectIsOpen = true;
        lastWasObjectEnd = false;
      } else if (isObjectEndWithPath(slice, i, schema.getFullPath())) {
        objectIsOpen = false;
        if (!lastWasObjectEnd) {
          lastWasObjectEnd = true;
        }
      } else if (slice.get(i) instanceof FeatureTokenType && arrayIsOpen && !objectIsOpen) {
        transformed.add(FeatureTokenType.ARRAY_END);
        transformed.add(schema.getFullPath());
        lastWasObjectEnd = false;
        arrayIsOpen = false;
      }

      transformed.add(slice.get(i));

      if (i == slice.size() - 1 && arrayIsOpen && lastWasObjectEnd) {
        transformed.add(FeatureTokenType.ARRAY_END);
        transformed.add(schema.getFullPath());
      }
    }

    return transformed;
  }

  private List<Object> wrapWithObject(List<Object> slice) {
    List<Object> transformed = new ArrayList<>();
    boolean lastWasChildOfPath = false;

    for (int i = 0; i < slice.size(); i++) {
      if (isChildOfPath(slice, i, schema.getFullPath())) {
        if (!lastWasChildOfPath) {
          transformed.add(FeatureTokenType.OBJECT);
          transformed.add(schema.getFullPath());
          lastWasChildOfPath = true;
        }
      } else if (slice.get(i) instanceof FeatureTokenType && lastWasChildOfPath) {
        transformed.add(FeatureTokenType.OBJECT_END);
        transformed.add(schema.getFullPath());
        lastWasChildOfPath = false;
      }
      transformed.add(slice.get(i));

      if (i == slice.size() - 1 && lastWasChildOfPath) {
        transformed.add(FeatureTokenType.OBJECT_END);
        transformed.add(schema.getFullPath());
      }
    }

    return transformed;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private List<Object> wrapSingleValuesWithObject(List<Object> slice, boolean wrapEachValue) {
    List<Object> transformed = new ArrayList<>();
    boolean lastWasChildOfPath = false;

    for (int i = 0; i < slice.size(); i++) {
      if (isChildOfPath(slice, i, schema.getFullPath())) {
        if (lastWasChildOfPath && wrapEachValue) {
          transformed.add(FeatureTokenType.OBJECT_END);
          transformed.add(schema.getFullPath());
          transformed.add(FeatureTokenType.OBJECT);
          transformed.add(schema.getFullPath());
        }
        if (!lastWasChildOfPath) {
          transformed.add(FeatureTokenType.OBJECT);
          transformed.add(schema.getFullPath());
          lastWasChildOfPath = true;
        }
      } else if (slice.get(i) instanceof FeatureTokenType && lastWasChildOfPath) {
        transformed.add(FeatureTokenType.OBJECT_END);
        transformed.add(schema.getFullPath());
        lastWasChildOfPath = false;
      }
      transformed.add(slice.get(i));

      if (i == slice.size() - 1 && lastWasChildOfPath) {
        transformed.add(FeatureTokenType.OBJECT_END);
        transformed.add(schema.getFullPath());
      }
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
