/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.json.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.ii.xtraplatform.features.domain.NestingTrackerBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JsonBuilder {

  private final ObjectMapper jsonMapper;
  private final NestingTrackerBase<Object> nestingTracker;
  private final Map<String, Object> currentDocument;

  private Map<String, Object> currentObject;
  private List<Object> currentArray;

  public JsonBuilder() {
    this.jsonMapper = new ObjectMapper();
    this.nestingTracker = new NestingTrackerBase<>();
    this.currentDocument = new LinkedHashMap<>();
  }

  public String build() throws IOException {
    return jsonMapper.writeValueAsString(currentDocument);
  }

  public void openObject(List<String> path) {
    if (nestingTracker.isNested() && nestingTracker.doesNotStartWithPreviousPath(path)) {
      error(path, "O", true);
    }
    if (nestingTracker.inArray() && !nestingTracker.isSamePath(path)) {
      error(path, "O", true);
    }

    Map<String, Object> property = createObject(path);

    nestingTracker.openObject(path, property);
  }

  public void closeObject(List<String> path) {
    if (!nestingTracker.inObject() || !nestingTracker.isSamePath(path)) {
      error(path, "O", false);
    }

    nestingTracker.closeObject();

    currentObject = null;

    restoreParent();
  }

  public void openArray(List<String> path) {
    if (nestingTracker.isNested() && nestingTracker.doesNotStartWithPreviousPath(path)) {
      error(path, "A", true);
    }
    if (nestingTracker.inArray()) {
      error(path, "A", true);
    }

    List<Object> property = createArray(path);

    nestingTracker.openArray(path, property);
  }

  public void closeArray(List<String> path) {
    if (!nestingTracker.inArray() || !nestingTracker.isSamePath(path)) {
      error(path, "A", false);
    }

    nestingTracker.closeArray();

    currentArray = null;

    restoreParent();
  }

  public void addValue(List<String> path, String value) {
    if (Objects.isNull(value)) {
      return;
    }

    createValue(path, value);
  }

  private void attachProperty(List<String> path, Object property) {
    if (nestingTracker.inObject()) {
      currentObject.put(path.get(path.size() - 1), property);
    } else if (nestingTracker.inArray()) {
      currentArray.add(property);
    } else {
      currentDocument.put(path.get(0), property);
    }
  }

  private Map<String, Object> createObject(List<String> path) {
    Map<String, Object> property = new LinkedHashMap<>();

    attachProperty(path, property);

    currentObject = property;

    return property;
  }

  private List<Object> createArray(List<String> path) {
    List<Object> property = new ArrayList<>();

    attachProperty(path, property);

    currentArray = property;

    return property;
  }

  private String createValue(List<String> path, String value) {
    String property = value;

    attachProperty(path, property);

    return property;
  }

  private void restoreParent() {
    if (nestingTracker.inObject()) {
      currentObject = (Map<String, Object>) nestingTracker.getCurrentPayload();
    } else if (nestingTracker.inArray()) {
      currentArray = (List<Object>) nestingTracker.getCurrentPayload();
    }
  }

  private void error(List<String> path, String type, boolean isOpen) {
    String msg =
        String.format(
            "Cannot %s %s with path %s. Current nesting stack:\n%s",
            isOpen ? "open" : "close", type, path, nestingTracker);

    throw new IllegalStateException(msg);
  }
}
