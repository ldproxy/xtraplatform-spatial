/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Captures per-feature {@code (PRIMARY_INTERVAL_START, PRIMARY_INTERVAL_END)} tuples and surfaces
 * them via {@link FeatureStream.Result#getVersionIntervals()} / {@link
 * FeatureStream.ResultReduced#getVersionIntervals()}. Used by the Time Map endpoint to enumerate
 * the versions of a feature without having to decode the full feature payload.
 *
 * <p>{@link Tuple#second()} is {@code null} for the open version (no end timestamp).
 */
public class FeatureTokenTransformerVersionIntervals extends FeatureTokenTransformer {

  private final Consumer<List<Tuple<Instant, Instant>>> versionsSetter;
  private final List<Tuple<Instant, Instant>> versions = new ArrayList<>();
  private String currentStart;
  private String currentEnd;

  public FeatureTokenTransformerVersionIntervals(ImmutableResult.Builder resultBuilder) {
    this.versionsSetter = resultBuilder::addAllVersionIntervals;
  }

  public <X> FeatureTokenTransformerVersionIntervals(
      ImmutableResultReduced.Builder<X> resultBuilder) {
    this.versionsSetter = resultBuilder::addAllVersionIntervals;
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.currentStart = null;
    this.currentEnd = null;
    super.onFeatureStart(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (Objects.nonNull(context.value())) {
      if (context.schema().filter(SchemaBase::isPrimaryIntervalStart).isPresent()) {
        currentStart = context.value();
      } else if (context.schema().filter(SchemaBase::isPrimaryIntervalEnd).isPresent()) {
        currentEnd = context.value();
      }
    }
    super.onValue(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (Objects.nonNull(currentStart)) {
      try {
        Instant start = Instant.parse(currentStart);
        Instant end = Objects.nonNull(currentEnd) ? Instant.parse(currentEnd) : null;
        versions.add(Tuple.of(start, end));
      } catch (Throwable ignore) {
        // skip rows with non-parseable timestamps
      }
    }
    super.onFeatureEnd(context);
  }

  @Override
  public void onEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    if (!versions.isEmpty()) {
      versionsSetter.accept(versions);
    }
    super.onEnd(context);
  }
}
