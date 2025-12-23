/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTokenTransformerLogger extends FeatureTokenTransformer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTokenTransformerLogger.class);

  @Override
  public void onFeatureStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("START FEATURE {} {}", context.pathAsString(), context.indexes());

    super.onFeatureStart(context);
  }

  @Override
  public void onFeatureEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("END FEATURE {} {}", context.pathAsString(), context.indexes());

    super.onFeatureEnd(context);
  }

  @Override
  public void onObjectStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("START OBJECT {} {}", context.pathAsString(), context.indexes());

    super.onObjectStart(context);
  }

  @Override
  public void onObjectEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("END OBJECT {} {}", context.pathAsString(), context.indexes());

    super.onObjectEnd(context);
  }

  @Override
  public void onArrayStart(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("START ARRAY {}", context.pathAsString());

    super.onArrayStart(context);
  }

  @Override
  public void onArrayEnd(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("END ARRAY {}", context.pathAsString());

    super.onArrayEnd(context);
  }

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    LOGGER.debug("GEOMETRY {}", context.pathAsString());

    super.onGeometry(context);
  }

  @Override
  public void onValue(ModifiableContext<FeatureSchema, SchemaMapping> context) {

    super.onValue(context);
  }
}
