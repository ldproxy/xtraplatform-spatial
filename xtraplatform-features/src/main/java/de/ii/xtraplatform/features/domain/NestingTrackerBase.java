/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zahnen
 */
@SuppressWarnings("PMD.GodClass")
public class NestingTrackerBase<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(NestingTrackerBase.class);

  private final List<String> nestingStack;
  private final List<List<String>> pathStack;
  private final List<T> payloadStack;

  public NestingTrackerBase() {
    this.nestingStack = new ArrayList<>();
    this.pathStack = new ArrayList<>();
    this.payloadStack = new ArrayList<>();
  }

  public void reset() {
    nestingStack.clear();
    pathStack.clear();
    payloadStack.clear();
  }

  public void openArray(List<String> path) {
    openArray(path, null);
  }

  public void openArray(List<String> path, T payload) {
    push("A", List.copyOf(path), payload);
  }

  public void openObject(List<String> path) {
    openObject(path, null);
  }

  public void openObject(List<String> path, T payload) {
    push("O", List.copyOf(path), payload);
  }

  public void closeAuto(List<String> path) {
    while (isNested() && doesNotStartWithPreviousPath(path)) {

      if (inObject()) {
        closeObject();
      } else if (inArray()) {
        closeArray();
      }
    }
  }

  public void closeObject() {
    if (nestingStack.isEmpty() || !Objects.equals(nestingStack.get(nestingStack.size() - 1), "O")) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No object to close");
      }
      return;
    }

    pop();
  }

  public void closeArray() {
    if (nestingStack.isEmpty() || !Objects.equals(nestingStack.get(nestingStack.size() - 1), "A")) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("No array to close");
      }
      return;
    }

    pop();
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

  private void push(String type, List<String> path, T payload) {
    nestingStack.add(type);
    pathStack.add(path);
    payloadStack.add(payload);
  }

  private void pop() {
    nestingStack.remove(nestingStack.size() - 1);
    pathStack.remove(pathStack.size() - 1);
    payloadStack.remove(payloadStack.size() - 1);
  }

  public List<String> getCurrentNestingPath() {
    if (pathStack.isEmpty()) {
      return List.of();
    }
    return pathStack.get(pathStack.size() - 1);
  }

  public List<String> relativize(List<String> path) {
    if (pathStack.isEmpty() || doesNotStartWithPreviousPath(path)) {
      return path;
    }
    if (isSamePath(path)) {
      return List.of();
    }

    List<String> parent = pathStack.get(pathStack.size() - 1);

    return path.subList(parent.size(), path.size());
  }

  public T getCurrentPayload() {
    if (payloadStack.isEmpty()) {
      return null;
    }
    return payloadStack.get(payloadStack.size() - 1);
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

  private static <T> boolean startsWith(List<T> longer, List<T> shorter) {
    if (Objects.isNull(longer) || Objects.isNull(shorter) || longer.size() < shorter.size()) {
      return false;
    }

    return Objects.equals(longer.subList(0, shorter.size()), shorter);
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < pathStack.size(); i++) {
      s.append(pathStack.get(i)).append(' ').append(nestingStack.get(i)).append('\n');
    }
    return s.toString();
  }
}
