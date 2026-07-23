/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

/**
 * Drops features whose id has already been emitted. The tokens of a feature are buffered until its
 * id property arrives; the feature is then either replayed or discarded as a whole. Duplicates can
 * only arise when multiple queries of a multi-query select the same feature.
 */
public class FeatureTokenTransformerDeduplicate extends FeatureTokenTransformer {

  public static final int MAX_FEATURES = 16_777_216;

  private final PackedIdSet seen;
  private final boolean idsArePerType;

  private final Queue<FeatureTokenType> tokenQueue;
  private final Queue<List<String>> pathQueue;
  private final Queue<Integer> schemaIndexQueue;
  private final Queue<List<Integer>> indexesQueue;
  private final Queue<String> valueQueue;
  private final Queue<Type> valueTypeQueue;
  private final Queue<Geometry<?>> geoQueue;
  private final Queue<Boolean> inArrayQueue;
  private final Queue<Boolean> inObjectQueue;

  private boolean buffering;
  private boolean dropping;
  private Optional<String> currentType;

  public FeatureTokenTransformerDeduplicate(boolean idsArePerType) {
    super();
    this.seen = new PackedIdSet(MAX_FEATURES);
    this.idsArePerType = idsArePerType;
    this.tokenQueue = new LinkedList<>();
    this.pathQueue = new LinkedList<>();
    this.schemaIndexQueue = new LinkedList<>();
    this.indexesQueue = new LinkedList<>();
    this.valueQueue = new LinkedList<>();
    this.valueTypeQueue = new LinkedList<>();
    this.geoQueue = new LinkedList<>();
    this.inArrayQueue = new LinkedList<>();
    this.inObjectQueue = new LinkedList<>();
    this.buffering = false;
    this.dropping = false;
    this.currentType = Optional.empty();
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.currentType =
        Objects.nonNull(context.mapping())
            ? Optional.of(context.mapping().getTargetSchema().getName())
            : Optional.empty();
    this.buffering = true;
    this.dropping = false;

    buffer(context, FeatureTokenType.FEATURE);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      this.dropping = false;
      return;
    }
    if (buffering) {
      // a feature without an id is always emitted
      flush(context);
    }

    super.onFeatureEnd(context);
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.OBJECT);
      return;
    }
    super.onObjectStart(context);
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.OBJECT_END);
      return;
    }
    super.onObjectEnd(context);
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.ARRAY);
      return;
    }
    super.onArrayStart(context);
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.ARRAY_END);
      return;
    }
    super.onArrayEnd(context);
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.GEOMETRY);
      return;
    }
    super.onGeometry(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (dropping) {
      return;
    }
    if (buffering) {
      buffer(context, FeatureTokenType.VALUE);
      handleIdValue(context);
      return;
    }
    super.onValue(context);
  }

  private void handleIdValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (context.schema().filter(SchemaBase::isId).isEmpty() || Objects.isNull(context.value())) {
      return;
    }

    String key =
        idsArePerType && currentType.isPresent()
            ? currentType.get() + ":" + context.value()
            : context.value();

    if (seen.add(key)) {
      flush(context);
    } else {
      clear();
      this.dropping = true;
      this.buffering = false;
    }
  }

  private void buffer(
      ModifiableContext<FeatureSchema, SchemaMapping> context, FeatureTokenType token) {
    tokenQueue.add(token);
    pathQueue.add(List.copyOf(context.path()));
    schemaIndexQueue.add(context.schemaIndex());
    indexesQueue.add(new ArrayList<>(context.indexes()));
    valueQueue.add(context.value());
    valueTypeQueue.add(context.valueType());
    geoQueue.add(context.geometry());
    inArrayQueue.add(context.inArray());
    inObjectQueue.add(context.inObject());
  }

  private void clear() {
    tokenQueue.clear();
    pathQueue.clear();
    schemaIndexQueue.clear();
    indexesQueue.clear();
    valueQueue.clear();
    valueTypeQueue.clear();
    geoQueue.clear();
    inArrayQueue.clear();
    inObjectQueue.clear();
  }

  private void flush(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.buffering = false;

    List<String> path = context.path();
    int schemaIndex = context.schemaIndex();
    List<Integer> indexes = new ArrayList<>(context.indexes());
    String value = context.value();
    Type valueType = context.valueType();
    Geometry<?> geometry = context.geometry();
    boolean inArray = context.inArray();
    boolean inObject = context.inObject();

    while (!tokenQueue.isEmpty()) {
      FeatureTokenType token = tokenQueue.remove();

      context.pathTracker().track(pathQueue.remove());
      context.setSchemaIndex(schemaIndexQueue.remove());
      context.setIndexes(indexesQueue.remove());
      context.setValue(valueQueue.remove());
      context.setValueType(valueTypeQueue.remove());
      context.setGeometry(geoQueue.remove());
      context.setInArray(inArrayQueue.remove());
      context.setInObject(inObjectQueue.remove());

      push(context, token);
    }

    context.pathTracker().track(path);
    context.setSchemaIndex(schemaIndex);
    context.setIndexes(indexes);
    context.setValue(value);
    context.setValueType(valueType);
    context.setGeometry(geometry);
    context.setInArray(inArray);
    context.setInObject(inObject);
  }

  private void push(
      ModifiableContext<FeatureSchema, SchemaMapping> context, FeatureTokenType token) {
    switch (token) {
      case FEATURE:
        super.onFeatureStart(context);
        break;
      case VALUE:
        super.onValue(context);
        break;
      case GEOMETRY:
        super.onGeometry(context);
        break;
      case OBJECT:
        super.onObjectStart(context);
        break;
      case OBJECT_END:
        super.onObjectEnd(context);
        break;
      case ARRAY:
        super.onArrayStart(context);
        break;
      case ARRAY_END:
        super.onArrayEnd(context);
        break;
      default:
        break;
    }
  }
}
