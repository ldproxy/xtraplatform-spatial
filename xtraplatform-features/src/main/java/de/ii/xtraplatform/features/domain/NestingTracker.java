/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.base.Joiner;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.GodClass")
public class NestingTracker {

  private static final Logger LOGGER = LoggerFactory.getLogger(NestingTracker.class);

  private final FeatureEventHandler<?, ?, ModifiableContext<?, ?>> downstream;
  private final ModifiableContext<?, ?> context;
  // a Set so the per-value isNotMain() membership test is O(1) instead of a linear scan of
  // List<List<String>> with element-wise list equality
  private final Set<List<String>> mainPaths;
  private final boolean flattenObjects;
  private final boolean flattenArrays;
  private final List<String> nestingStack;
  private final List<List<String>> pathStack;
  private final List<String> flattened;
  private final boolean skippable;

  public <T extends ModifiableContext<?, ?>> NestingTracker(
      FeatureEventHandler<?, ?, T> downstream,
      T context,
      Set<List<String>> mainPaths,
      boolean flattenObjects,
      boolean flattenArrays,
      boolean skippable) {
    this.downstream = (FeatureEventHandler<?, ?, ModifiableContext<?, ?>>) downstream;
    this.context = context;
    this.mainPaths = mainPaths;
    this.flattenObjects = flattenObjects;
    this.flattenArrays = flattenArrays;
    this.skippable = skippable;
    this.nestingStack = new ArrayList<>();
    this.pathStack = new ArrayList<>();
    this.flattened = new ArrayList<>();
  }

  public void openArray() {
    if (flattenArrays) {
      flattened.add(context.schema().get().getName());
    } else {
      if (!skippable || !context.shouldSkip()) {
        downstream.onArrayStart(context);
      }
    }

    push("A", context.pathTracker().asList());
    context.setInArray(true);
  }

  public void openObject() {
    if (flattenArrays && inArray()) {
      flattened.add(String.valueOf(context.index()));
    } else if (flattenObjects && (flattenArrays || !inArray())) {
      flattened.add(context.schema().get().getName());
    } else {
      if (!skippable || !context.shouldSkip()) {
        downstream.onObjectStart(context);
      }
    }

    push("O", context.pathTracker().asList());
    context.setInObject(true);
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  public void closeObject() {
    if (nestingStack.isEmpty() || !Objects.equals(nestingStack.get(nestingStack.size() - 1), "O")) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No object to close");
      }
      return;
    }

    context.pathTracker().track(getCurrentNestingPath());

    if (flattenArrays && isObjectInArray()) {
      flattened.remove(flattened.size() - 1);
    } else if (flattenObjects && (flattenArrays || !isObjectInArray())) {
      flattened.remove(flattened.size() - 1);
    } else {
      if (!skippable || !context.shouldSkip()) {
        downstream.onObjectEnd(context);
      }
    }

    pop();

    if (!nestingStack.contains("O")) {
      context.setInObject(false);
    }
    context.pathTracker().track(getCurrentNestingPath());
  }

  public void closeArray() {
    if (nestingStack.isEmpty() || !Objects.equals(nestingStack.get(nestingStack.size() - 1), "A")) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No array to close");
      }
      return;
    }
    context.pathTracker().track(getCurrentNestingPath());
    if (flattenArrays) {
      flattened.remove(flattened.size() - 1);
    } else {
      if (!skippable || !context.shouldSkip()) {
        downstream.onArrayEnd(context);
      }
    }

    pop();

    if (!nestingStack.contains("A")) {
      context.setInArray(false);
    }
    if (!pathStack.isEmpty()) {
      context.pathTracker().track(getCurrentNestingPath());
    }
  }

  public int arrayDepth() {
    return (int) nestingStack.stream().filter("A"::equals).count();
  }

  public void close() {
    if (nestingStack.isEmpty()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No object or array to close");
      }
      return;
    }
    if (inObject()) {
      closeObject();
    } else if (inArray()) {
      closeArray();
    }
  }

  private void push(String type, List<String> path) {
    nestingStack.add(type);
    pathStack.add(path);
  }

  private void pop() {
    nestingStack.remove(nestingStack.size() - 1);
    pathStack.remove(pathStack.size() - 1);
  }

  public List<String> getCurrentNestingPath() {
    if (pathStack.isEmpty()) {
      return List.of();
    }
    return pathStack.get(pathStack.size() - 1);
  }

  public boolean isNested() {
    return !getCurrentNestingPath().isEmpty();
  }

  public boolean inArray() {
    return isNested() && Objects.equals(nestingStack.get(nestingStack.size() - 1), "A");
  }

  public boolean inObject() {
    return isNested() && Objects.equals(nestingStack.get(nestingStack.size() - 1), "O");
  }

  public boolean isObjectInArray() {
    return inObject()
        && nestingStack.size() > 1
        && Objects.equals(nestingStack.get(nestingStack.size() - 2), "A");
  }

  public boolean isNotMain(List<String> nextPath) {
    return !mainPaths.contains(nextPath);
  }

  public boolean isFirst(List<Integer> indexes) {
    return !indexes.isEmpty() && indexes.get(indexes.size() - 1) == 1;
  }

  public boolean isSamePath(List<String> nextPath) {
    return Objects.equals(getCurrentNestingPath(), nextPath);
  }

  public boolean doesNotStartWithPreviousPath(List<String> nextPath) {
    return !startsWith(nextPath, getCurrentNestingPath());
  }

  public boolean doesStartWithPreviousPath(List<String> nextPath) {
    return !Objects.equals(nextPath, getCurrentNestingPath())
        && startsWith(nextPath, getCurrentNestingPath());
  }

  public boolean hasIndexChanged(List<Integer> nextIndexes) {
    return nextIndexes.size() >= context.indexes().size()
        && !startsWith(nextIndexes, context.indexes());
  }

  public boolean hasParentIndexChanged(List<Integer> nextIndexes) {
    return nextIndexes.size() > 1
        && context.indexes().size() >= nextIndexes.size() - 1
        && nextIndexes.get(nextIndexes.size() - 2) > context.indexes().get(nextIndexes.size() - 2);
  }

  private static <T> boolean startsWith(List<T> longer, List<T> shorter) {
    if (Objects.isNull(longer) || Objects.isNull(shorter) || longer.size() < shorter.size()) {
      return false;
    }

    return Objects.equals(longer.subList(0, shorter.size()), shorter);
  }

  public String getFlattenedPropertyPath(String separator, String name) {
    if (inArray() && !context.indexes().isEmpty()) {
      flattened.add(String.valueOf(context.index()));
    } else {
      flattened.add(name);
    }
    String path = Joiner.on(separator).join(flattened);
    flattened.remove(flattened.size() - 1);
    return path;
  }
}
