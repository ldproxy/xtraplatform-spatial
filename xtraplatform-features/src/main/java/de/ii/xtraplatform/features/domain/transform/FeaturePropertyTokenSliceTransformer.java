/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.MappingOperationResolver;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.Tuple;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

public interface FeaturePropertyTokenSliceTransformer
    extends FeaturePropertyTransformer<List<Object>> {

  FeatureSchema transformSchema(FeatureSchema schema);

  void transformObject(
      String currentPropertyPath,
      List<Object> slice,
      List<String> rootPath,
      int start,
      int end,
      List<Object> result);

  default void transformValue(List<Object> slice, int start, int end, List<Object> result) {}

  @Override
  default List<Object> transform(String currentPropertyPath, List<Object> slice) {
    return transformObjects(currentPropertyPath, slice);
  }

  default List<Object> transformObjects(String currentPropertyPath, List<Object> slice) {
    if (slice.isEmpty()) {
      return slice;
    }

    List<String> path = pathAsList(currentPropertyPath);
    int min = findFirst(slice, path, 0);
    int max = findLast(slice, path, min + 1);

    if (min == -1 || max == -1) {
      return slice;
    }

    List<Object> transformed = new ArrayList<>();

    transformed.addAll(slice.subList(0, min));

    int lastEnd = min - 1;
    int start = findPos(slice, FeatureTokenType.OBJECT, path, min);
    int end = findPos(slice, FeatureTokenType.OBJECT_END, path, start);

    while (start > -1 && end > -1 && end <= max) {
      if (start > lastEnd) {
        transformed.addAll(slice.subList(lastEnd + 1, start));
      }

      // TODO: +2 is needed for .sublist usage in implementations, should be +1 here and +1 in
      // implementations
      transformObject(currentPropertyPath, slice, path, start, end + 2, transformed);

      lastEnd = end + 1;
      start = findPos(slice, FeatureTokenType.OBJECT, path, lastEnd);
      end = findPos(slice, FeatureTokenType.OBJECT_END, path, start);
    }

    if (slice.size() > lastEnd) {
      transformed.addAll(slice.subList(lastEnd + 1, slice.size()));
    }

    return transformed;
  }

  default List<Object> transformValueArray(List<String> path, List<Object> slice) {
    if (slice.isEmpty()) {
      return slice;
    }

    int min = findFirst(slice, path, 0);
    int max = findLast(slice, path, min + 1);

    if (min == -1 || max == -1) {
      return slice;
    }

    List<Object> before = slice.subList(0, min);
    List<Object> after = slice.subList(max + 1, slice.size());
    boolean inArray = before.contains(FeatureTokenType.ARRAY);
    List<Object> transformed = new ArrayList<>();

    transformed.addAll(before);

    if (!inArray) {
      transformValue(slice, min, max + 1, transformed);
    } else {

      int start = findPos(slice, FeatureTokenType.ARRAY, path, min);
      int end = findPos(slice, FeatureTokenType.ARRAY_END, path, start);

      if (start > -1) {
        transformed.addAll(slice.subList(min, start));
      }

      while (start > -1 && end > -1 && end + 1 <= max) {
        transformValue(slice, start, end, transformed);

        start = findPos(slice, FeatureTokenType.ARRAY, path, end);

        if (start > -1) {
          transformed.addAll(slice.subList(end + 2, start));
        } else {
          transformed.addAll(slice.subList(end + 2, max + 1));
        }

        end = findPos(slice, FeatureTokenType.ARRAY_END, path, start);
      }
    }

    transformed.addAll(after);

    return transformed;
  }

  default List<Object> transformValues(List<String> path, boolean isArray, List<Object> slice) {
    if (slice.isEmpty()) {
      return slice;
    }

    int min = findFirst(slice, path, 0);
    int max = findLast(slice, path, min + 1);

    if (min == -1 || max == -1) {
      return slice;
    }

    List<Object> before = slice.subList(0, min);
    List<Object> after = slice.subList(max + 1, slice.size());
    boolean inArray = isArray || before.contains(FeatureTokenType.ARRAY);
    List<Object> transformed = new ArrayList<>();

    transformed.addAll(before);

    if (!inArray) {
      transformValue(slice, min, max + 1, transformed);
    } else {

      int start = findPos(slice, FeatureTokenType.VALUE, path, min);
      int end = findFirstNot(slice, FeatureTokenType.VALUE, path, start);

      if (start > -1) {
        transformed.addAll(slice.subList(min, start));
      }

      while (start > -1 && end > -1 && end <= max) {
        transformValue(slice, start, end, transformed);

        start = findPos(slice, FeatureTokenType.VALUE, path, end);

        if (start > -1) {
          transformed.addAll(slice.subList(end + 1, start));
        } else {
          transformed.addAll(slice.subList(end + 1, max + 1));
        }

        end = findFirstNot(slice, FeatureTokenType.VALUE, path, start);
      }
    }

    transformed.addAll(after);

    return transformed;
  }

  Joiner PATH_JOINER = Joiner.on('.');

  Splitter PATH_SPLITTER = Splitter.on('.');

  default String pathAsString(List<String> path) {
    return PATH_JOINER.join(path);
  }

  default List<String> pathAsList(String path) {
    return PATH_SPLITTER.splitToList(path);
  }

  default List<String> getRootPath(List<Object> slice) {
    if (slice.size() < 2
        || (slice.get(0) != FeatureTokenType.OBJECT && slice.get(0) != FeatureTokenType.ARRAY)
        || !(slice.get(1) instanceof List)) {
      throw new IllegalArgumentException("Not a valid object or array");
    }

    return (List<String>) slice.get(1);
  }

  default List<String> getRootPath(List<Object> slice, int min) {
    if (slice.size() < min + 2
        || (slice.get(min) != FeatureTokenType.OBJECT && slice.get(min) != FeatureTokenType.ARRAY)
        || !(slice.get(min + 1) instanceof List)) {
      throw new IllegalArgumentException("Not a valid object or array");
    }

    return (List<String>) slice.get(min + 1);
  }

  default void checkObject(FeatureSchema schema) {
    if (schema.getType() != Type.OBJECT && schema.getType() != Type.OBJECT_ARRAY) {
      throw new IllegalArgumentException(
          String.format(
              "Transformer %s can only be applied to OBJECT or OBJECT_ARRAY, found: %s",
              getType(), schema.getType()));
    }
  }

  default void checkValue(FeatureSchema schema) {
    if (!schema.isValue() || schema.isArray()) {
      throw new IllegalArgumentException(
          String.format(
              "Transformer %s can only be applied to VALUE, found: %s",
              getType(), schema.getType()));
    }
  }

  default void checkArray(FeatureSchema schema) {
    if (schema.getType() != Type.VALUE_ARRAY && schema.getType() != Type.OBJECT_ARRAY) {
      throw new IllegalArgumentException(
          String.format(
              "Transformer %s can only be applied to VALUE_ARRAY or OBJECT_ARRAY, found: %s",
              getType(), schema.getType()));
    }
  }

  default void checkValueArray(FeatureSchema schema) {
    if (schema.getType() != Type.VALUE_ARRAY) {
      throw new IllegalArgumentException(
          String.format(
              "Transformer %s can only be applied to VALUE_ARRAY, found: %s",
              getType(), schema.getType()));
    }
  }

  default int findPos(List<Object> slice, FeatureTokenType type, List<String> path, int offset) {
    if (offset == -1) {
      return -1;
    }

    for (int i = offset; i < slice.size() - 1; i++) {
      if (Objects.equals(slice.get(i), type) && Objects.equals(slice.get(i + 1), path)) {
        return i;
      }
    }

    return -1;
  }

  default int findFirstNot(
      List<Object> slice, FeatureTokenType type, List<String> path, int offset) {
    if (offset == -1) {
      return -1;
    }

    for (int i = offset; i < slice.size() - 1; i++) {
      if (slice.get(i) instanceof FeatureTokenType
          && (!Objects.equals(slice.get(i), type) || !Objects.equals(slice.get(i + 1), path))) {
        return i - 1;
      }
    }

    return slice.size() - 1;
  }

  default int findFirst(List<Object> slice, List<String> path, int offset) {
    if (offset == -1) {
      return -1;
    }

    for (int i = offset; i < slice.size() - 1; i++) {
      if (slice.get(i) instanceof FeatureTokenType && Objects.equals(slice.get(i + 1), path)) {
        return i;
      }
    }

    return -1;
  }

  default int findLast(List<Object> slice, List<String> path, int offset) {
    if (offset == -1) {
      return -1;
    }
    boolean last = false;
    int pos = -1;

    for (int i = offset; i < slice.size(); i++) {
      if (slice.get(i) instanceof FeatureTokenType && Objects.equals(slice.get(i + 1), path)) {
        last = true;
      } else if (last
          && slice.get(i) instanceof FeatureTokenType
          && !Objects.equals(slice.get(i + 1), path)) {
        last = false;
        pos = i - 1;
      } else if (last && i == slice.size() - 1) {
        pos = i;
      }
    }

    return pos;
  }

  default List<Object> valueSlice(List<String> path, @Nullable String value, Type type) {
    List<Object> slice = new ArrayList<>();
    slice.add(FeatureTokenType.VALUE);
    slice.add(path);
    slice.add(value);
    slice.add(type);

    return slice;
  }

  default Function<String, String> getValueLookup(
      String currentPropertyPath, Function<String, String> genericLookup, List<Object> slice) {
    return getValueLookup(currentPropertyPath, genericLookup, slice, 0, slice.size());
  }

  default Function<String, String> getValueLookup(
      String currentPropertyPath,
      Function<String, String> genericLookup,
      List<Object> slice,
      int from,
      int to) {
    Map<String, Integer> valueIndexes = getValueIndexes(slice, from, to);

    return key -> {
      if (Objects.isNull(key)) {
        return null;
      }

      String lookupWithKey = genericLookup.apply(key);

      if (Objects.nonNull(lookupWithKey)) {
        return lookupWithKey;
      }

      String fullKey = currentPropertyPath + "." + key;
      String lookupWithFullKey = genericLookup.apply(fullKey);

      if (Objects.nonNull(lookupWithFullKey)) {
        return lookupWithFullKey;
      }

      if (valueIndexes.containsKey(key)) {
        return getValue(slice, valueIndexes.get(key));
      }

      if (valueIndexes.containsKey(fullKey)) {
        return getValue(slice, valueIndexes.get(fullKey));
      }

      return null;
    };
  }

  default Function<String, String> getValueIndexLookup(
      List<String> path,
      Function<String, String> genericLookup,
      List<Object> slice,
      int from,
      int to) {
    Map<String, Integer> valueIndexes = getValueIndexesList(slice, from, to);

    return key -> {
      if (Objects.isNull(key)) {
        return null;
      }

      String lookupWithKey = genericLookup.apply(key);

      if (Objects.nonNull(lookupWithKey)) {
        return lookupWithKey;
      }

      String fullKey = joinPath(path) + "." + key;
      String lookupWithFullKey = genericLookup.apply(fullKey);

      if (Objects.nonNull(lookupWithFullKey)) {
        return lookupWithFullKey;
      }

      if (valueIndexes.containsKey(key)) {
        return getValue(slice, valueIndexes.get(key));
      }

      if (valueIndexes.containsKey(fullKey)) {
        return getValue(slice, valueIndexes.get(fullKey));
      }

      return null;
    };
  }

  default Map<String, Integer> getValueIndexes(List<Object> slice, int from, int to) {
    Map<String, Integer> valueIndexes = new LinkedHashMap<>();

    for (int i = from; i < to; i++) {
      if (slice.get(i) == FeatureTokenType.VALUE) {
        if (i + 2 < to && slice.get(i + 1) instanceof List) {
          valueIndexes.put(joinPath((List<String>) slice.get(i + 1)), i + 2);
        }
      }
    }

    return valueIndexes;
  }

  default Map<String, Integer> getValueIndexesByProp(
      List<Object> slice, int from, int to, int depth) {
    Map<String, Integer> valueIndexes = new LinkedHashMap<>();

    for (int i = from; i < to; i++) {
      if (slice.get(i) == FeatureTokenType.VALUE) {
        if (i + 2 < to
            && slice.get(i + 1) instanceof List
            && ((List<?>) slice.get(i + 1)).size() > depth) {
          valueIndexes.put(((List<String>) slice.get(i + 1)).get(depth), i + 2);
        }
      }
    }

    return valueIndexes;
  }

  default Map<String, Integer> getValueIndexesList(List<Object> slice, int from, int to) {
    Map<String, Integer> valueIndexes = new LinkedHashMap<>();
    int j = 0;

    for (int i = from; i < to; i++) {
      if (slice.get(i) == FeatureTokenType.VALUE) {
        if (i + 2 < to && slice.get(i + 1) instanceof List) {
          valueIndexes.put(Integer.toString(j++), i + 2);
        }
      }
    }

    return valueIndexes;
  }

  default boolean isTypeWithPath(
      List<Object> slice, int index, FeatureTokenType type, List<String> path) {
    return slice.get(index) == type
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isValueWithPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) == FeatureTokenType.VALUE
        && index + 3 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isNonNullValue(List<Object> slice, int index) {
    return slice.get(index) == FeatureTokenType.VALUE
        && index + 3 < slice.size()
        && Objects.nonNull(slice.get(index + 2));
  }

  default boolean isObjectWithPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) == FeatureTokenType.OBJECT
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isObjectEndWithPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) == FeatureTokenType.OBJECT_END
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isArrayWithPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) == FeatureTokenType.ARRAY
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isArrayEndWithPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) == FeatureTokenType.ARRAY_END
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && Objects.equals(slice.get(index + 1), path);
  }

  default boolean isChildOfPath(List<Object> slice, int index, List<String> path) {
    return slice.get(index) instanceof FeatureTokenType
        && index + 1 < slice.size()
        && slice.get(index + 1) instanceof List
        && ((List<?>) slice.get(index + 1)).size() > path.size()
        && Objects.equals(path, ((List<String>) slice.get(index + 1)).subList(0, path.size()));
  }

  static String joinPath(List<String> path) {
    String last = path.get(path.size() - 1);
    if (needsClean(last)) {
      List<String> path2 = new ArrayList<>(path.subList(0, path.size() - 1));
      path2.add(clean(last));

      return PATH_JOINER.join(path2);
    }

    return PATH_JOINER.join(path);
  }

  static String clean(String propertyPath) {
    return MappingOperationResolver.cleanConcatPath(propertyPath);
  }

  static boolean needsClean(String propertyPath) {
    return MappingOperationResolver.isConcatPath(propertyPath);
  }

  static String getValue(List<Object> slice, int valueIndex) {
    if (valueIndex < slice.size() && slice.get(valueIndex) instanceof String) {
      return (String) slice.get(valueIndex);
    }

    return null;
  }

  static int getValueIndex(List<Object> slice, String path, int from, int to) {
    for (int i = from; i < to; i++) {
      if (slice.get(i) == FeatureTokenType.VALUE) {
        if (i + 2 < to && slice.get(i + 1) instanceof List) {
          if (Objects.equals(path, joinPath((List<String>) slice.get(i + 1)))) {
            return i + 2;
          }
        }
      }
    }

    return -1;
  }

  default String getValue(List<Object> slice, String path) {
    return getValue(slice, path, 0, slice.size());
  }

  default String getValue(List<Object> slice, String path, int start, int end) {
    int valueIndex = getValueIndex(slice, path, start, end);

    if (valueIndex < 0) {
      return null;
    }

    return (String) slice.get(valueIndex);
  }

  default Tuple<String, Type> getValueAndType(List<Object> slice, String path) {
    return getValueAndType(slice, path, 0, slice.size());
  }

  default Tuple<String, Type> getValueAndType(List<Object> slice, String path, int start, int end) {
    int valueIndex = getValueIndex(slice, path, start, end);

    if (valueIndex < 0) {
      return Tuple.of(null, Type.STRING);
    }

    String value = (String) slice.get(valueIndex);
    Type type =
        slice.size() > valueIndex + 1 && slice.get(valueIndex + 1) instanceof Type
            ? (Type) slice.get(valueIndex + 1)
            : Type.STRING;

    return Tuple.of(value, type);
  }

  default Optional<FeatureSchema> findProperty(FeatureSchema schema, String propertyName) {
    return schema.getProperties().stream()
        .filter(
            property ->
                Objects.equals(property.getName(), propertyName)
                    || Objects.equals(clean(property.getName()), propertyName))
        .findFirst();
  }
}
