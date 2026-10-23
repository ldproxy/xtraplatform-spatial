/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import java.util.Set;

/**
 * Rejects a decoded feature that sets a read-only property, instead of discarding the value and
 * reporting a success that does not describe what was stored.
 *
 * <p>A decoder wraps the handler it emits to, so the check sees every event of every format in one
 * place and the paths are the property names of the feature type, which is how a decoder tracks
 * them.
 *
 * <p>Not applicable to a request body that was merged with the current feature, as for a JSON Merge
 * Patch: such a body carries the read-only properties of the current feature, whether or not the
 * client sent them, so the patch document has to be checked instead.
 */
public class FeatureEventHandlerReadOnly<T, U, V extends ModifiableContext<T, U>>
    implements FeatureEventHandlerSimple<T, U, V> {

  private final FeatureEventHandlerSimple<T, U, V> delegate;
  private final Set<String> readOnly;

  private FeatureEventHandlerReadOnly(
      FeatureEventHandlerSimple<T, U, V> delegate, Set<String> readOnly) {
    this.delegate = delegate;
    this.readOnly = readOnly;
  }

  /** The handler itself, if the feature type has no read-only property. */
  public static <T, U, V extends ModifiableContext<T, U>> FeatureEventHandlerSimple<T, U, V> of(
      FeatureEventHandlerSimple<T, U, V> delegate, Set<String> readOnly) {
    return readOnly.isEmpty() ? delegate : new FeatureEventHandlerReadOnly<>(delegate, readOnly);
  }

  /** Whether the path is a read-only property or a property of one. */
  public static boolean isReadOnly(Set<String> readOnly, String path) {
    return readOnly.stream()
        .anyMatch(readOnlyPath -> path.equals(readOnlyPath) || path.startsWith(readOnlyPath + "."));
  }

  private void reject(V context) {
    String path = context.pathAsString();

    if (isReadOnly(readOnly, path)) {
      throw new IllegalArgumentException(
          String.format(
              "The property '%s' is read-only, a request that changes a feature must not set it.",
              path));
    }
  }

  @Override
  public void onValue(V context) {
    reject(context);

    delegate.onValue(context);
  }

  @Override
  public void onObjectStart(V context) {
    reject(context);

    delegate.onObjectStart(context);
  }

  @Override
  public void onArrayStart(V context) {
    reject(context);

    delegate.onArrayStart(context);
  }

  @Override
  public void onGeometry(V context) {
    reject(context);

    delegate.onGeometry(context);
  }

  @Override
  public void onStart(V context) {
    delegate.onStart(context);
  }

  @Override
  public void onEnd(V context) {
    delegate.onEnd(context);
  }

  @Override
  public void onFeatureStart(V context) {
    delegate.onFeatureStart(context);
  }

  @Override
  public void onFeatureEnd(V context) {
    delegate.onFeatureEnd(context);
  }

  @Override
  public void onObjectEnd(V context) {
    delegate.onObjectEnd(context);
  }

  @Override
  public void onArrayEnd(V context) {
    delegate.onArrayEnd(context);
  }
}
