/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FeatureTokenReaderSimple<T, U, V extends ModifiableContext<T, U>> {

  private final FeatureEventHandlerSimple<T, U, V> eventHandler;

  private FeatureTokenType currentType;
  private int contextIndex;
  private V context;
  private List<String> nestingStack;
  private Map<List<String>, Integer> schemaIndexes;

  public FeatureTokenReaderSimple(FeatureEventHandlerSimple<T, U, V> eventHandler, V context) {
    this.eventHandler = eventHandler;
    this.context = context;
    this.nestingStack = new ArrayList<>();
    this.schemaIndexes = new HashMap<>();
  }

  public void onToken(Object token) {
    if (token instanceof FeatureTokenType) {
      if (Objects.nonNull(currentType)) {
        emitEvent();
        this.context.setSchemaIndex(-1);
      }
      if (token == FeatureTokenType.FLUSH) {
        this.currentType = null;
      }
      initEvent((FeatureTokenType) token);
    } else {
      readContext(token);
    }

    if (token == FeatureTokenType.INPUT_END) {
      emitEvent();
    }
  }

  private void initEvent(FeatureTokenType token) {
    this.currentType = token;
    this.contextIndex = 0;

    context.pathTracker().track(0);
    context.setValueType(Type.UNKNOWN);
    context.setValue(null);

    switch (currentType) {
      case FEATURE, FLUSH:
        schemaIndexes.replaceAll((p, i) -> -1);
        break;
      case OBJECT:
        schemaIndexes.replaceAll((p, i) -> -1);
        this.context.setInObject(true);
        if (inArray()) {
          this.context
              .indexes()
              .set(
                  this.context.indexes().size() - 1,
                  this.context.indexes().get(this.context.indexes().size() - 1) + 1);
        }
        push("O");
        break;
      case ARRAY:
        schemaIndexes.replaceAll((p, i) -> -1);
        this.context.setInArray(true);
        this.context.indexes().add(0);
        push("A");
        break;
      case VALUE:
        if (inArray()) {
          this.context
              .indexes()
              .set(
                  this.context.indexes().size() - 1,
                  this.context.indexes().get(this.context.indexes().size() - 1) + 1);
        }
        break;
      case ARRAY_END:
        this.context.indexes().remove(this.context.indexes().size() - 1);
        pop();
        if (!nestingStack.contains("A")) {
          this.context.setInArray(false);
        }
        break;
      case OBJECT_END:
        pop();
        if (!nestingStack.contains("O")) {
          this.context.setInObject(false);
        }
        break;
      case GEOMETRY:
        break;
    }
  }

  private void readContext(Object context) {
    switch (currentType) {
      case INPUT:
        if (contextIndex == 0 && context instanceof Boolean) {
          this.context.metadata().isSingleFeature((Boolean) context);
        } else if (contextIndex == 0 && context instanceof Long) {
          this.context.metadata().numberReturned((Long) context);
        } else if (contextIndex == 1 && context instanceof Long) {
          this.context.metadata().numberMatched((Long) context);
        }
        break;
      case FEATURE, OBJECT, ARRAY, ARRAY_END, OBJECT_END:
        tryReadPath(context);
        break;
      case GEOMETRY:
        tryReadPath(context);
        if (contextIndex == 0 && context instanceof List) {
          schemaIndexes.compute((List<String>) context, (k, v) -> (v == null) ? 0 : v + 1);
          this.context.setSchemaIndex(schemaIndexes.get((List<String>) context));
        }
        if (contextIndex == 1 && context instanceof Geometry) {
          this.context.setGeometry((Geometry<?>) context);
        }
        break;
      case VALUE:
        tryReadPath(context);
        if (!inArray() && contextIndex == 0 && context instanceof List) {
          schemaIndexes.compute((List<String>) context, (k, v) -> (v == null) ? 0 : v + 1);
          this.context.setSchemaIndex(schemaIndexes.get((List<String>) context));
        }
        if (contextIndex == 1 && context instanceof String) {
          this.context.setValue((String) context);
        } else if (contextIndex == 2 && context instanceof SchemaBase.Type) {
          this.context.setValueType((Type) context);
        }
        break;
      case FEATURE_END:
      case INPUT_END:
        break;
    }

    this.contextIndex++;
  }

  private void tryReadPath(Object context) {
    if (contextIndex == 0 && context instanceof List) {
      this.context.pathTracker().track((List<String>) context);
    }
  }

  private void emitEvent() {
    switch (currentType) {
      case INPUT:
        eventHandler.onStart(context);
        break;
      case FEATURE:
        eventHandler.onFeatureStart(context);
        break;
      case OBJECT:
        eventHandler.onObjectStart(context);
        break;
      case ARRAY:
        eventHandler.onArrayStart(context);
        break;
      case VALUE:
        eventHandler.onValue(context);
        break;
      case GEOMETRY:
        eventHandler.onGeometry(context);
        break;
      case ARRAY_END:
        eventHandler.onArrayEnd(context);
        break;
      case OBJECT_END:
        eventHandler.onObjectEnd(context);
        break;
      case FEATURE_END:
        eventHandler.onFeatureEnd(context);
        break;
      case INPUT_END:
        eventHandler.onEnd(context);
        break;
    }
  }

  private boolean inArray() {
    return !nestingStack.isEmpty() && nestingStack.get(nestingStack.size() - 1).equals("A");
  }

  private void push(String type) {
    nestingStack.add(type);
  }

  private void pop() {
    nestingStack.remove(nestingStack.size() - 1);
  }
}
