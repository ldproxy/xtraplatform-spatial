/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;

/**
 * Rejects a decoded feature that sets an empty value — a string with no characters or with only
 * whitespace — instead of storing it as though the client had supplied something.
 *
 * <p>A decoder wraps the handler it emits to, so the check rides the decoding of the request body:
 * it sees the values of every format in one place, without a second parse of the content, and it
 * sees them as the decoder resolved them, so a value carried by an attribute rather than by element
 * content needs no special case of its own.
 *
 * <p>Only a value event can carry an empty value. A value that the document omits, or states as
 * null, does not reach the handler at all, so the absence of a value is unaffected — and only a
 * string can be empty, so where the request body is also validated against a schema this check adds
 * exactly what the schema cannot express.
 */
public class FeatureEventHandlerEmptyValues<T, U, V extends ModifiableContext<T, U>>
    implements FeatureEventHandlerSimple<T, U, V> {

  private final FeatureEventHandlerSimple<T, U, V> delegate;

  private FeatureEventHandlerEmptyValues(FeatureEventHandlerSimple<T, U, V> delegate) {
    this.delegate = delegate;
  }

  /** The handler itself, unless empty values are rejected. */
  public static <T, U, V extends ModifiableContext<T, U>> FeatureEventHandlerSimple<T, U, V> of(
      FeatureEventHandlerSimple<T, U, V> delegate, boolean rejectEmptyValues) {
    return rejectEmptyValues ? new FeatureEventHandlerEmptyValues<>(delegate) : delegate;
  }

  /**
   * Whether the value is empty: a string with no characters or with only whitespace. {@code null}
   * is the absence of a value, not an empty one, and is therefore not empty.
   */
  public static boolean isEmpty(String value) {
    return value != null && value.isBlank();
  }

  /** The rejection of an empty value, so every call site reports one the same way. */
  public static IllegalArgumentException rejected(String path) {
    return new IllegalArgumentException(
        String.format(
            "The property '%s' has an empty value, a request that changes a feature must not set"
                + " one. Omit the property, or state it as null, to leave it without a value.",
            path));
  }

  @Override
  public void onValue(V context) {
    if (isEmpty(context.value())) {
      throw rejected(context.pathAsString());
    }

    delegate.onValue(context);
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
  public void onObjectStart(V context) {
    delegate.onObjectStart(context);
  }

  @Override
  public void onObjectEnd(V context) {
    delegate.onObjectEnd(context);
  }

  @Override
  public void onArrayStart(V context) {
    delegate.onArrayStart(context);
  }

  @Override
  public void onArrayEnd(V context) {
    delegate.onArrayEnd(context);
  }

  @Override
  public void onGeometry(V context) {
    delegate.onGeometry(context);
  }
}
