/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.geometries.domain.Geometry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
NOTE: while this works, it is cumbersome and hard to maintain
 a much cleaner solution would use FeatureTokenEmitter and FeatureTokenReader for buffering
 but these are out of sync with Context and have to be adjusted (geoDim, in(Geo|Array|Object))
*/
public class FeatureTokenTransformerSorting extends FeatureTokenTransformer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FeatureTokenTransformerSorting.class);

  private final Map<String, Map<List<String>, Integer>> pathIndex;
  private final Map<String, Map<List<String>, Integer>> rearrange;
  private final Queue<Integer> indexQueue;
  private final Queue<List<String>> pathQueue;
  private final Queue<Integer> schemaIndexQueue;
  private final Queue<List<Integer>> indexesQueue;
  private final Queue<String> valueQueue;
  private final Queue<Geometry<?>> geoQueue;
  private final Queue<Boolean> inArrayQueue;
  private final Queue<Boolean> inObjectQueue;
  private final Queue<FeatureTokenType> tokenQueue;
  private final Queue<Type> valueTypeQueue;
  /*private FeatureTokenBuffer<
      FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
  downstream;*/

  private String currentType;
  private int bufferIndex;
  private List<String> bufferParent;

  public FeatureTokenTransformerSorting() {
    this.pathIndex = new LinkedHashMap<>();
    this.rearrange = new LinkedHashMap<>();
    this.indexQueue = new LinkedList<>();
    this.pathQueue = new LinkedList<>();
    this.schemaIndexQueue = new LinkedList<>();
    this.indexesQueue = new LinkedList<>();
    this.valueQueue = new LinkedList<>();
    this.geoQueue = new LinkedList<>();
    this.inArrayQueue = new LinkedList<>();
    this.inObjectQueue = new LinkedList<>();
    this.tokenQueue = new LinkedList<>();
    this.valueTypeQueue = new LinkedList<>();
    this.currentType = null;
    this.bufferIndex = 0;
    this.bufferParent = new ArrayList<>();
  }

  @Override
  protected void init() {
    // this.downstream = new FeatureTokenBuffer<>(getDownstream(), getContext());

    super.init();
  }

  @Override
  public void onStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    analyzeMapping(context.mapping());

    super.onStart(context);
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    analyzeMapping(context.mapping());

    super.onFeatureStart(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (bufferIndex > 0) {
      emptyBuffer(context, Integer.MAX_VALUE);
      // downstream.bufferStop(true);
    }

    super.onFeatureEnd(context);
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int index = getIndex(context.path());

    checkBuffer(context, FeatureTokenType.OBJECT, index, index > bufferIndex);
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    boolean isBufferParent = startsWith(bufferParent, context.path());
    int triggerIndex = 0;
    if (isBufferParent) {
      triggerIndex = findLastStartsWith(context.path()) + 1;
    }

    checkBuffer(context, FeatureTokenType.OBJECT_END, triggerIndex, isBufferParent);
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int index = getIndex(context.path());

    checkBuffer(context, FeatureTokenType.ARRAY, index, index > bufferIndex);
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int index = getIndex(context.path());

    checkBuffer(context, FeatureTokenType.ARRAY_END, index, false);
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int index = getIndex(context.path());

    checkBuffer(context, FeatureTokenType.GEOMETRY, index, index > bufferIndex);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    int index = getIndex(context.path());

    checkBuffer(context, FeatureTokenType.VALUE, index, index > bufferIndex);
  }

  private int getIndex(List<String> path) {
    if (Objects.nonNull(currentType)) {
      return Objects.requireNonNullElse(pathIndex.get(currentType).get(path), -1);
    }
    return -1;
  }

  // TODO: identify out of order elements, on element after ooe start buffer and mark 0, on ooe
  // insert 0 and flush
  // TODO: multiple ooes following each other, multiple marks
  // TODO: test with xleit, create unit tests
  private void analyzeMapping(SchemaMapping mapping) {
    if (Objects.isNull(mapping)) {
      return;
    }

    this.currentType = mapping.getTargetSchema().getName();

    if (pathIndex.containsKey(currentType) || rearrange.containsKey(currentType)) {
      return;
    }

    pathIndex.put(currentType, new LinkedHashMap<>());
    rearrange.put(currentType, new LinkedHashMap<>());

    int index = 0;
    List<String> lastParent = null;
    boolean doRearrange = false;

    for (List<String> path : mapping.getSchemasByTargetPath().keySet()) {
      if (path.size() > 1) {
        if (path.stream().anyMatch(elem -> elem.matches("\\[[^=\\]]+].+"))) {
          continue;
        }
        boolean isNested = path.size() > 2 || path.get(path.size() - 1).startsWith("[");
        List<String> parent = path.subList(0, path.size() - 1);
        if (lastParent == null) {
          lastParent = parent;
        }

        if (!doRearrange && !Objects.equals(parent, lastParent) && !isNested) {
          doRearrange = true;
        } else if (doRearrange && (!Objects.equals(parent, lastParent) || isNested)) {
          doRearrange = false;
        }

        lastParent = parent;

        if (doRearrange) {
          this.rearrange.get(currentType).put(path, index);
        }
      }

      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("{}: {}{}", index, doRearrange ? "QUEUE " : "", path);
      }

      this.pathIndex.get(currentType).put(path, index);
      index++;
    }
  }

  private void checkBuffer(
      ModifiableContext<FeatureSchema, SchemaMapping> context,
      FeatureTokenType token,
      int triggerIndex,
      boolean doEmptyBuffer) {
    int index = getIndex(context.path());
    boolean doRearrange =
        rearrange.containsKey(currentType)
            && rearrange.get(currentType).containsKey(context.path());

    if (doRearrange) {
      buffer(context, index, token);
      /*if (!downstream.isBuffering()) {
        if (bufferIndex <= 0) {
          this.bufferIndex = index;
          this.bufferParent = context.path().subList(0, context.path().size() - 1);
        }
        downstream.bufferStart();
        downstream.bufferMark();
        push(context, token);
      }*/
    } else {
      if (bufferIndex > 0 && doEmptyBuffer) {
        emptyBuffer(context, triggerIndex);
        // downstream.bufferFlush();
      }
      // downstream.bufferStop(false);
      push(context, token);
    }
  }

  private static boolean startsWith(List<String> a, List<String> b) {
    return Objects.equals(a, b)
        || (a.size() > b.size() && Objects.equals(a.subList(0, b.size()), b));
  }

  private int findLastStartsWith(List<String> parent) {
    if (!pathIndex.containsKey(currentType)) {
      return -1;
    }
    return pathIndex.get(currentType).entrySet().stream()
        .filter(entry -> startsWith(entry.getKey(), parent))
        .mapToInt(Entry::getValue)
        .max()
        .orElse(-1);
  }

  private void buffer(
      ModifiableContext<FeatureSchema, SchemaMapping> context, int index, FeatureTokenType token) {
    if (bufferIndex <= 0) {
      this.bufferIndex = index;
      this.bufferParent = context.path().subList(0, context.path().size() - 1);
    }

    indexQueue.add(index);
    tokenQueue.add(token);
    pathQueue.add(context.path());
    schemaIndexQueue.add(context.schemaIndex());
    indexesQueue.add(new ArrayList<>(context.indexes()));
    valueQueue.add(context.value());
    geoQueue.add(context.geometry());
    inArrayQueue.add(context.inArray());
    inObjectQueue.add(context.inObject());
    valueTypeQueue.add(context.valueType());
  }

  private void emptyBuffer(
      ModifiableContext<FeatureSchema, SchemaMapping> context, int triggerIndex) {

    List<String> path = context.path();
    int schemaIndex = context.schemaIndex();
    ArrayList<Integer> indexes = new ArrayList<>(context.indexes());
    String value = context.value();
    Geometry<?> geometry = context.geometry();
    boolean inArray = context.inArray();
    boolean inObject = context.inObject();

    while (!indexQueue.isEmpty() && indexQueue.peek() < triggerIndex) {
      int index = indexQueue.remove();
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
    context.setGeometry(geometry);
    context.setInArray(inArray);
    context.setInObject(inObject);

    this.bufferIndex = Objects.requireNonNullElse(indexQueue.peek(), 0);
    this.bufferParent =
        Objects.nonNull(pathQueue.peek()) && pathQueue.peek().size() > 1
            ? pathQueue.peek().subList(0, pathQueue.peek().size() - 1)
            : new ArrayList<>();
  }

  private void push(
      ModifiableContext<FeatureSchema, SchemaMapping> context, FeatureTokenType token) {
    switch (token) {
      case VALUE:
        // downstream.onValue(context);
        super.onValue(context);
        break;
      case GEOMETRY:
        // downstream.onGeometry(context);
        super.onGeometry(context);
        break;
      case OBJECT:
        // downstream.onObjectStart(context);
        super.onObjectStart(context);
        break;
      case OBJECT_END:
        // downstream.onObjectEnd(context);
        super.onObjectEnd(context);
        break;
      case ARRAY:
        // downstream.onArrayStart(context);
        super.onArrayStart(context);
        break;
      case ARRAY_END:
        // downstream.onArrayEnd(context);
        super.onArrayEnd(context);
        break;
    }
  }
}
