/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenValidator extends FeatureTokenTransformer {

  static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenValidator.class);

  private final NestingTrackerBase<Object> nestingTracker;

  public FeatureTokenValidator() {
    this.nestingTracker = new NestingTrackerBase<>();
  }

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("START FEATURE {} {}", context.pathAsString(), context.indexes());

    super.onFeatureStart(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("END FEATURE {} {}", context.pathAsString(), context.indexes());

    super.onFeatureEnd(context);
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("START OBJECT {} {}", context.pathAsString(), context.indexes());

    if (nestingTracker.isNested() && nestingTracker.doesNotStartWithPreviousPath(context.path())) {
      error(context.path(), Type.object, true);
    }
    if (nestingTracker.inArray() && !nestingTracker.isSamePath(context.path())) {
      error(context.path(), Type.object, true);
    }

    nestingTracker.openObject(context.path());

    super.onObjectStart(context);
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("END OBJECT {} {}", context.pathAsString(), context.indexes());

    if (!nestingTracker.inObject() || !nestingTracker.isSamePath(context.path())) {
      error(context.path(), Type.object, false);
    }

    nestingTracker.closeObject();

    super.onObjectEnd(context);
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("START ARRAY {}", context.pathAsString());
    if (nestingTracker.isNested() && nestingTracker.doesNotStartWithPreviousPath(context.path())) {
      error(context.path(), Type.array, true);
    }
    if (nestingTracker.inArray()) {
      error(context.path(), Type.array, true);
    }

    nestingTracker.openArray(context.path());

    super.onArrayStart(context);
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("END ARRAY {}", context.pathAsString());
    if (!nestingTracker.inArray() || !nestingTracker.isSamePath(context.path())) {
      error(context.path(), Type.array, false);
    }

    nestingTracker.closeArray();

    super.onArrayEnd(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("VALUE {} {}", context.pathAsString(), context.value());
    if (nestingTracker.isNested() && nestingTracker.doesNotStartWithPreviousPath(context.path())) {
      error(context.path(), Type.value, true);
    }
    if (nestingTracker.inArray() && !nestingTracker.isSamePath(context.path())) {
      error(context.path(), Type.value, true);
    }

    super.onValue(context);
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.trace("GEOMETRY {} {}", context.pathAsString(), context.geometry());

    super.onGeometry(context);
  }

  private enum Type {
    object,
    array,
    value
  }

  private void error(List<String> path, Type type, boolean isOpen) {
    String msg =
        String.format(
            "Cannot %s %s with path %s. Current nesting stack:\n%s",
            isOpen ? "open" : "close", type, path, nestingTracker);

    throw new IllegalStateException(msg);
  }
}
