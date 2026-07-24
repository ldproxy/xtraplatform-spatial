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
import java.util.stream.Collectors;

/**
 * Per-feature token buffer with two distinct responsibilities:
 *
 * <ol>
 *   <li><b>A position-addressable slice index</b> for the in-buffer token-slice transformers
 *       (flatten, concat, codelist, …): {@link #getSlice(int)}/{@link #replaceSlice(int, List)}
 *       read and rewrite a property - or an enclosing object together with its descendants - by its
 *       schema position. The index ({@link #start(int)}/{@link #length(int)}) is derived on demand
 *       from the buffered tokens by {@link #computeIndex()}; the transformers rewrite within a
 *       property and do not reorder across properties.
 *   <li><b>Emission order</b>, applied by the single {@link #orderedBySchema(List)} pass.
 * </ol>
 *
 * <p>{@link #append(Object)} stores tokens in the order the provider produces them, which is the
 * provider's per-table order, not the schema order: a property backed by a joined table (object,
 * object array, value array, feature reference) is produced after the main table's columns even
 * when it is declared before them, and a property produced from several tables arrives as several
 * fragments. {@link #ensureOrdered()} applies the schema-order pass in place, lazily and exactly
 * once per feature: before the slice transformers read the buffer - so each property's fragments
 * are contiguous and a transformer's slice cannot swallow an unrelated property emitted between
 * them - and at the latest at flush when no transformer ran.
 */
@SuppressWarnings({"PMD.GodClass", "PMD.CyclomaticComplexity", "PMD.TooManyMethods"})
public class FeatureEventBuffer<
        U extends SchemaBase<U>, V extends SchemaMappingBase<U>, W extends ModifiableContext<U, V>>
    implements FeatureTokenEmitter2<U, V, W> {

  private final FeatureEventHandler<U, V, W> downstream;
  private final List<Object> buffer;
  private final FeatureTokenEmitter2<U, V, W> bufferIn;
  private final FeatureTokenReader<U, V, W> bufferOut;

  private final int[] events;
  private final List<List<Integer>> enclosings;
  private final Map<String, SchemaMapping> mappings;
  private boolean doBuffer;
  private boolean indexStale;
  private boolean schemaOrdered;
  private String lastType;

  public FeatureEventBuffer(
      FeatureEventHandler<U, V, W> downstream, W context, Map<String, SchemaMapping> mappings) {
    this.downstream = downstream;
    this.buffer = new ArrayList<>();
    this.bufferIn = this::append;
    this.bufferOut = new FeatureTokenReader<>(downstream, context);
    this.mappings = mappings;

    this.doBuffer = false;
    this.indexStale = true;
    this.schemaOrdered = false;

    int maxEvents =
        mappings.values().stream()
                    .mapToInt(SchemaMappingBase::getNumberOfTargets)
                    .max()
                    .orElseThrow()
                * 2
            + 2;
    this.events = new int[maxEvents];
    this.enclosings = new ArrayList<>(Collections.nCopies(maxEvents, List.of()));
  }

  public FeatureTokenEmitter2<U, V, W> getBuffer() {
    return bufferIn;
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
   * Updates the index after a slice changed size: grows the length of {@code pos} by {@code delta}
   * and shifts the start of every later position by the same amount. The buffer is in
   * schema-position order when this runs (see {@link #ensureOrdered()}), so a resized slice moves
   * every position after it by {@code delta} - this keeps the index correct without a full {@link
   * #computeIndex()} rebuild per slice rewrite.
   */
  private void plus(int pos, int delta) {
    events[(pos * 2) + 1] += delta;
    for (int i = (pos + 1) * 2; i < events.length; i += 2) {
      events[i] += delta;
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

  /**
   * Buffers a token in the order the provider produces it. No ordering is attempted here; {@link
   * #orderedBySchema(List)} applies schema order later, on demand. The buffer is marked unordered
   * and its slice index stale, both rebuilt lazily the next time a slice is read, written, or
   * flushed.
   */
  void append(Object token) {
    buffer.add(token);
    schemaOrdered = false;
    indexStale = true;
  }

  /**
   * Re-sorts the buffer into schema order in place, once. This must run before the in-buffer slice
   * transformers read or rewrite a slice: a property produced by the provider as several per-table
   * fragments (e.g. a concatenated object array) is contiguous only after sorting, and {@link
   * #getSlice(int)} hands a transformer a contiguous buffer range - without this, an unrelated
   * property emitted between two fragments would be swallowed into the slice. It is also the single
   * place schema order is applied for the final emission. The slice transformers preserve schema
   * order (they rewrite within a property), so it is not repeated after they run.
   */
  private void ensureOrdered() {
    if (schemaOrdered) {
      return;
    }
    List<Object> ordered = orderedBySchema(buffer);
    buffer.clear();
    buffer.addAll(ordered);
    schemaOrdered = true;
    indexStale = true;
  }

  /**
   * Rebuilds the position-addressable slice index from the buffered tokens in a single pass. Each
   * token group is accrued to its outermost enclosing position ({@link #minPos(int, List)} of the
   * token's position and its ancestors, or the token's own position when it is top-level) - the
   * property whose slice encloses it. An enclosing object's range therefore spans all of its
   * descendants even when the object's own marker is not emitted (e.g. a flattened source object),
   * which is exactly the slice {@link #getSlice(int)} hands to a transformer. Runs only when a
   * slice is actually accessed, so features without slice transformers never pay for it.
   */
  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private void computeIndex() {
    Arrays.fill(events, 0);
    indexStale = false;

    SchemaMapping mapping = Objects.isNull(lastType) ? null : mappings.get(lastType);
    if (Objects.isNull(mapping) || buffer.isEmpty()) {
      return;
    }

    int i = 0;
    while (i < buffer.size()) {
      int j = i + 1;
      while (j < buffer.size() && !(buffer.get(j) instanceof FeatureTokenType)) {
        j++;
      }
      List<String> path =
          i + 1 < buffer.size() && buffer.get(i + 1) instanceof List
              ? (List<String>) buffer.get(i + 1)
              : List.of();
      int pos = positionForPath(mapping, path);
      if (pos >= 0) {
        setSpan(minPos(pos, enclosings.get(pos)), i, j);
      }

      i = j;
    }

    // Positions without tokens (an absent property, or a nested position whose tokens were accrued
    // to its enclosing) keep length 0, but still need a valid buffer offset as their start:
    // getSlice
    // hands out buffer.subList(start, end) and replaceSlice inserts at start, and the incremental
    // plus() shift after a slice shrinks moves every later position - including these - by the same
    // delta. Left at 0, an empty position after a shrunk slice is driven negative and subList
    // throws.
    // The buffer is in schema-position order here, so a forward scan yields monotonic, non-negative
    // starts; only top-level (enclosing) positions carry a non-zero length, so nextStart advances
    // exactly across the buffer.
    int nextStart = 0;
    for (int pos = 0; pos < events.length / 2; pos++) {
      if (length(pos) == 0) {
        events[pos * 2] = nextStart;
      } else {
        nextStart = end(pos);
      }
    }
  }

  /**
   * Records the buffer range [start, end) for a schema position. A position can occur more than
   * once at the same level - object-array elements share the array's position, and several
   * consecutive arrays at the same path are merged by the concat transformer - so repeated spans
   * are unioned into the enclosing range rather than overwritten. The occurrences are contiguous in
   * production order, so the union is the full span the transformer expects.
   */
  private void setSpan(int pos, int start, int end) {
    if (pos < 0) {
      return;
    }
    int length = length(pos);
    if (length == 0) {
      events[pos * 2] = start;
      events[(pos * 2) + 1] = end - start;
    } else {
      int unionStart = Math.min(start(pos), start);
      int unionEnd = Math.max(end(pos), end);
      events[pos * 2] = unionStart;
      events[(pos * 2) + 1] = unionEnd - unionStart;
    }
  }

  private int positionForPath(SchemaMapping mapping, List<String> path) {
    if (path.isEmpty()) {
      return -1;
    }
    List<Integer> positions = mapping.getPositionsForTargetPath(path);
    return positions.isEmpty() ? -1 : positions.get(0);
  }

  void reset(String type) {
    Arrays.fill(events, 0);
    this.indexStale = true;
    this.schemaOrdered = false;

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
    ensureOrdered();
    buffer.forEach(bufferOut::onToken);
    bufferOut.onToken(FeatureTokenType.FLUSH);
    buffer.clear();
  }

  /**
   * Re-sorts the buffered feature so that properties are emitted in the order declared in the
   * schema, regardless of the order in which the provider produced them. The buffer holds tokens in
   * the provider's per-table order, not the schema order: a property backed by a joined table
   * (object, object array, value array, feature reference) is produced after the columns of the
   * main table even when it is declared before them. The pass rebuilds the token stream as a tree
   * and serialises each object's children in schema-position order; array elements keep their
   * (data) order, and the children inside each element are ordered by schema position like any
   * other object. {@link #ensureOrdered()} applies it in place, before the slice transformers read
   * the buffer (so each property's fragments are contiguous) and at the latest by flush.
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

  @SuppressWarnings({
    "PMD.AvoidInstantiatingObjectsInLoops",
    "PMD.CognitiveComplexity",
    "PMD.CyclomaticComplexity"
  })
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
      coalesceSplitObjects(node, mapping);
    }
    for (Node child : node.children) {
      orderChildren(child, mapping);
    }
  }

  /**
   * Coalesces adjacent OBJECT children that share a schema position into one object. The provider
   * produces a single-valued object backed by more than one table as several per-table fragments -
   * separate {@code OBJECT[path]…OBJECT_END[path]} blocks at the same position - which must become
   * one object. Object-array elements are never affected: they are children of an ARRAY node, which
   * is excluded from sorting and coalescing by the caller. Runs after the sort, so the fragments
   * are already adjacent; the merged children are ordered by the recursive {@link #orderChildren}
   * pass.
   */
  private static void coalesceSplitObjects(Node node, SchemaMapping mapping) {
    List<Node> coalesced = new ArrayList<>(node.children.size());
    for (Node child : node.children) {
      Node previous = coalesced.isEmpty() ? null : coalesced.get(coalesced.size() - 1);
      if (Objects.nonNull(previous)
          && previous.type == FeatureTokenType.OBJECT
          && child.type == FeatureTokenType.OBJECT
          && positionOf(child, mapping) != Integer.MAX_VALUE
          && positionOf(previous, mapping) == positionOf(child, mapping)) {
        previous.children.addAll(child.children);
      } else {
        coalesced.add(child);
      }
    }
    node.children.clear();
    node.children.addAll(coalesced);
  }

  private static int positionOf(Node node, SchemaMapping mapping) {
    if (node.position != Node.POSITION_UNCOMPUTED) {
      return node.position;
    }
    List<String> path = node.path();
    int position;
    if (path.isEmpty()) {
      position = Integer.MAX_VALUE;
    } else {
      List<Integer> positions = mapping.getPositionsForTargetPath(path);
      int p = positions.isEmpty() ? -1 : positions.get(0);
      // keep paths without a known position at the end, in their original (stable) order
      position = p < 0 ? Integer.MAX_VALUE : p;
    }
    node.position = position;
    return position;
  }

  private static final class Node {
    private static final int POSITION_UNCOMPUTED = Integer.MIN_VALUE;

    private final FeatureTokenType type;
    private final List<Object> open;
    private final List<Node> children = new ArrayList<>();
    private List<Object> close;
    // schema position (positionOf), memoized for the duration of one ordering pass: the sort
    // comparator and the coalesce pass would otherwise re-resolve the same path on every comparison
    private int position = POSITION_UNCOMPUTED;

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

    ensureOrdered();

    if (pos == 0) {
      return Collections.unmodifiableList(buffer);
    }

    if (indexStale) {
      computeIndex();
    }

    int enclosing = minPos(pos, enclosings.get(pos));

    List<Object> slice = buffer.subList(start(enclosing), end(enclosing));

    return Collections.unmodifiableList(slice);
  }

  public boolean replaceSlice(int pos, List<Object> replacement) {
    if (pos < 0) {
      return false;
    }

    ensureOrdered();

    if (pos == 0) {
      if (Objects.equals(buffer, replacement)) {
        return false;
      }
      buffer.clear();
      buffer.addAll(replacement);
      // the whole buffer was replaced; rebuild the index before the next slice access
      indexStale = true;
      return true;
    }

    if (indexStale) {
      computeIndex();
    }

    int enclosing = minPos(pos, enclosings.get(pos));
    List<Object> slice = buffer.subList(start(enclosing), end(enclosing));

    if (Objects.equals(slice, replacement)) {
      return false;
    }

    int delta = replacement.size() - slice.size();
    slice.clear();
    slice.addAll(replacement);

    // keep the index correct in place instead of rebuilding it for every slice rewrite
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

  @Override
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
