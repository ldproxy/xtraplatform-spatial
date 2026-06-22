/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureTokenEmitter2;
import de.ii.xtraplatform.features.domain.FeatureTokenReader;
import de.ii.xtraplatform.features.domain.FeatureTokenType;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.features.domain.SchemaMappingBase;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Vector;
import java.util.stream.Collectors;

public class FeatureEventBuffer<
        U extends SchemaBase<U>, V extends SchemaMappingBase<U>, W extends ModifiableContext<U, V>>
    implements FeatureTokenEmitter2<U, V, W> {

  private final FeatureEventHandler<U, V, W> downstream;
  private final List<Object> buffer;
  private final FeatureTokenEmitter2<U, V, W> bufferIn;
  private final FeatureTokenReader<U, V, W> bufferOut;

  private final int[] events;
  private final Vector<List<Integer>> enclosings;
  private final Map<String, SchemaMapping> mappings;
  private boolean doBuffer;
  public int current;
  public List<Integer> currentEnclosing;
  private String lastType;

  public FeatureEventBuffer(
      FeatureEventHandler<U, V, W> downstream, W context, Map<String, SchemaMapping> mappings) {
    this.downstream = downstream;
    this.buffer = new ArrayList<>();
    this.bufferIn = (FeatureTokenEmitter2<U, V, W>) (this::append);
    this.bufferOut = new FeatureTokenReader<>(downstream, context);
    this.enclosings = new Vector<>();
    this.mappings = mappings;

    this.doBuffer = false;
    this.current = 0;
    this.currentEnclosing = List.of();

    int maxEvents =
        mappings.values().stream()
                    .mapToInt(SchemaMappingBase::getNumberOfTargets)
                    .max()
                    .orElseThrow()
                * 2
            + 2;
    this.events = new int[maxEvents];
    enclosings.setSize(maxEvents);
  }

  public FeatureTokenEmitter2<U, V, W> getBuffer() {
    return bufferIn;
  }

  public void next(int pos) {
    next(pos, List.of());
  }

  public void next(int pos, List<Integer> enclosing) {
    this.current = pos;
    this.currentEnclosing = enclosing;
  }

  /**
   * An event consists of 1 to n tokens and is saved in the buffer. An event has a desired position
   * that must not match the order of occurrence. events contains the buffer start index and token
   * count for every event by position.
   *
   * @param pos event position
   * @return first index for event position in buffer
   */
  private int start(int pos) {
    return events[pos * 2];
  }

  /**
   * @param pos event position
   * @return length for event position in buffer
   */
  private int length(int pos) {
    return events[(pos * 2) + 1];
  }

  /**
   * @param pos event position
   * @return last index for event position in buffer
   */
  private int end(int pos) {
    return start(pos) + length(pos);
  }

  /**
   * Increase length for given event position in buffer.
   *
   * @param pos event position
   */
  private void increase(int pos) {
    plus(pos, 1);
  }

  private void increase(int pos, List<Integer> enclosing) {
    plus(pos, 1);

    for (int pos2 : enclosing) {
      plus(pos2, 1, false);
    }
  }

  private void plus(int pos, int delta) {
    plus(pos, delta, true);
  }

  private void plus(int pos, int delta, boolean propagate) {
    // increase length of pos
    int lenPos = (pos * 2) + 1;
    events[lenPos] += delta;

    // increase start of following pos
    if (propagate) {
      for (int i = (pos + 1) * 2; i < events.length; i += 2) {
        events[i] += delta;
      }
    }
  }

  /**
   * Positions in enclosing are always smaller than pos, the last position in enclosing is the
   * smallest.
   *
   * @param pos
   * @param enclosing
   * @return
   */
  private int minPos(int pos, List<Integer> enclosing) {
    if (enclosing.isEmpty()) {
      return pos;
    }
    return enclosing.get(enclosing.size() - 1);
  }

  void append(Object token) {
    int end = end(current);
    buffer.add(end, token);

    int minPos = minPos(current, currentEnclosing);

    increase(minPos);
  }

  void reset(String type) {
    Arrays.fill(events, 0);

    if (!Objects.equals(lastType, type)) {
      Collections.fill(enclosings, List.of());

      this.lastType = type;

      SchemaMapping mapping = mappings.get(lastType);

      for (Entry<List<String>, List<Integer>> entry :
          mapping.getPositionsByTargetPath().entrySet()) {
        List<String> path = entry.getKey();
        List<Integer> pos = entry.getValue();

        enclosings.set(pos.get(0), mapping.getParentPositionsForTargetPath(path).get(0));
      }
    }
  }

  public void bufferStart() {
    this.doBuffer = true;
  }

  public void bufferStop(boolean flush) {
    this.doBuffer = false;
    if (flush) {
      bufferFlush();
    }
  }

  public void bufferFlush() {
    List<Object> ordered = orderedBySchema(buffer);
    ordered.add(FeatureTokenType.FLUSH);
    ordered.forEach(bufferOut::onToken);
    buffer.clear();
  }

  /**
   * Re-sorts the buffered feature so that properties are emitted in the order declared in the
   * schema, regardless of the order in which the provider produced them. The incremental buffer
   * accounting places a property where its tokens first arrive, which is the SQL provider's
   * per-table order, not the schema order: a property backed by a joined table (object, object
   * array, value array, feature reference) is produced after the columns of the main table even
   * when it is declared before them. Running after the slice transformers, this pass rebuilds the
   * token stream as a tree and serialises each object's children in schema-position order. Array
   * elements keep their (data) order; the children inside each element are ordered by schema
   * position like any other object.
   */
  private List<Object> orderedBySchema(List<Object> tokens) {
    SchemaMapping mapping = Objects.isNull(lastType) ? null : mappings.get(lastType);

    if (Objects.isNull(mapping) || tokens.isEmpty()) {
      return new ArrayList<>(tokens);
    }

    Node root = new Node(null, List.of());
    buildTree(tokens, root);
    orderChildren(root, mapping);

    List<Object> ordered = new ArrayList<>(tokens.size());
    for (Node child : root.children) {
      child.flattenInto(ordered);
    }
    return ordered;
  }

  private static void buildTree(List<Object> tokens, Node root) {
    Deque<Node> stack = new ArrayDeque<>();
    stack.push(root);

    int i = 0;
    while (i < tokens.size()) {
      // a token group is a marker (FeatureTokenType) followed by its context tokens up to the next
      // marker
      int j = i + 1;
      while (j < tokens.size() && !(tokens.get(j) instanceof FeatureTokenType)) {
        j++;
      }
      List<Object> group = new ArrayList<>(tokens.subList(i, j));
      FeatureTokenType type =
          tokens.get(i) instanceof FeatureTokenType ? (FeatureTokenType) tokens.get(i) : null;

      if (type == FeatureTokenType.OBJECT || type == FeatureTokenType.ARRAY) {
        Node node = new Node(type, group);
        stack.peek().children.add(node);
        stack.push(node);
      } else if (type == FeatureTokenType.OBJECT_END || type == FeatureTokenType.ARRAY_END) {
        if (stack.size() > 1) {
          stack.pop().close = group;
        }
      } else {
        stack.peek().children.add(new Node(type, group));
      }

      i = j;
    }
  }

  private static void orderChildren(Node node, SchemaMapping mapping) {
    // array elements keep their data order; the children of any object (including each array
    // element) are ordered by their schema position
    if (node.type != FeatureTokenType.ARRAY && node.children.size() > 1) {
      node.children.sort(Comparator.comparingInt(child -> positionOf(child, mapping)));
    }
    for (Node child : node.children) {
      orderChildren(child, mapping);
    }
  }

  private static int positionOf(Node node, SchemaMapping mapping) {
    List<String> path = node.path();
    if (path.isEmpty()) {
      return Integer.MAX_VALUE;
    }
    List<Integer> positions = mapping.getPositionsForTargetPath(path);
    int position = positions.isEmpty() ? -1 : positions.get(0);
    // keep paths without a known position at the end, in their original (stable) order
    return position < 0 ? Integer.MAX_VALUE : position;
  }

  private static final class Node {
    private final FeatureTokenType type;
    private final List<Object> open;
    private final List<Node> children = new ArrayList<>();
    private List<Object> close;

    private Node(FeatureTokenType type, List<Object> open) {
      this.type = type;
      this.open = open;
    }

    @SuppressWarnings("unchecked")
    private List<String> path() {
      return open.size() > 1 && open.get(1) instanceof List
          ? (List<String>) open.get(1)
          : List.of();
    }

    private void flattenInto(List<Object> out) {
      out.addAll(open);
      for (Node child : children) {
        child.flattenInto(out);
      }
      if (Objects.nonNull(close)) {
        out.addAll(close);
      }
    }
  }

  public boolean isBuffering() {
    return doBuffer;
  }

  public List<Object> getSlice(int pos) {
    if (pos < 0) {
      return List.of();
    }
    if (pos == 0) {
      return Collections.unmodifiableList(buffer);
    }

    int enclosing = minPos(pos, enclosings.get(pos));

    List<Object> slice = buffer.subList(start(enclosing), end(enclosing));

    /*if (slice.isEmpty() && !enclosings.get(pos).isEmpty()) {
      for (int pos2: enclosings.get(pos)) {
        slice = buffer.subList(start(pos2), end(pos2));
        if (!slice.isEmpty()) {
          break;
        }
      }
    }*/

    return Collections.unmodifiableList(slice);
  }

  public boolean replaceSlice(int pos, List<Object> replacement) {
    if (pos < 0) {
      return false;
    }

    int enclosing = minPos(pos, enclosings.get(pos));

    List<Object> slice = pos == 0 ? buffer : buffer.subList(start(enclosing), end(enclosing));

    if (Objects.equals(slice, replacement)) {
      return false;
    }

    int delta = replacement.size() - slice.size();

    slice.clear();
    slice.addAll(replacement);

    if (delta != 0) {
      plus(enclosing, delta);
    }

    return true;
  }

  @Override
  public void push(Object token) {
    append(token);
  }

  @Override
  public void onStart(W context) {
    if (doBuffer) {
      bufferIn.onStart(context);
    } else {
      downstream.onStart(context);
    }
  }

  @Override
  public void onEnd(W context) {
    if (doBuffer) {
      bufferIn.onEnd(context);
    } else {
      downstream.onEnd(context);
    }
  }

  @Override
  public void onFeatureStart(W context) {
    reset(context.type());

    if (doBuffer) {
      bufferIn.onFeatureStart(context);
    } else {
      downstream.onFeatureStart(context);
    }
  }

  @Override
  public void onFeatureEnd(W context) {
    if (doBuffer) {
      bufferIn.onFeatureEnd(context);
    } else {
      downstream.onFeatureEnd(context);
    }
  }

  @Override
  public void onObjectStart(W context) {
    if (doBuffer) {
      bufferIn.onObjectStart(context);
    } else {
      downstream.onObjectStart(context);
    }
  }

  @Override
  public void onObjectEnd(W context) {
    if (doBuffer) {
      bufferIn.onObjectEnd(context);
    } else {
      downstream.onObjectEnd(context);
    }
  }

  @Override
  public void onArrayStart(W context) {
    if (doBuffer) {
      bufferIn.onArrayStart(context);
    } else {
      downstream.onArrayStart(context);
    }
  }

  @Override
  public void onArrayEnd(W context) {
    if (doBuffer) {
      bufferIn.onArrayEnd(context);
    } else {
      downstream.onArrayEnd(context);
    }
  }

  @Override
  public void onGeometry(W context) {
    if (doBuffer) {
      bufferIn.onGeometry(context);
    } else {
      downstream.onGeometry(context);
    }
  }

  @Override
  public void onValue(W context) {
    if (doBuffer) {
      bufferIn.onValue(context);
    } else {
      downstream.onValue(context);
    }
  }

  public String toString() {
    return sliceToString(buffer);
  }

  public static String sliceToString(List<Object> slice) {
    return slice.stream()
        .map(
            token -> {
              if (token instanceof FeatureTokenType) {
                return "FeatureTokenType." + token;
              }
              if (token instanceof SchemaBase.Type) {
                return "Type." + token;
              }
              if (token instanceof GeometryType) {
                return "GeometryType." + token;
              }
              if (token instanceof String) {
                return "\"" + token + "\"";
              }
              if (token instanceof List) {
                return ((List<?>) token)
                    .stream()
                        .map(elem -> "\"" + elem + "\"")
                        .collect(Collectors.joining(", ", "[", "]"));
              }
              if (token == null) {
                return "null";
              }
              return token.toString();
            })
        .collect(Collectors.joining(",\n"));
  }
}
