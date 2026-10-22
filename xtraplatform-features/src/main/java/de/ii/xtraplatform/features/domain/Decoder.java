/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;

public interface Decoder extends AutoCloseable {

  void decode(byte[] data, Pipeline pipeline);

  default void reset() {
    reset(true);
  }

  default void reset(boolean full) {}

  interface Pipeline {
    ModifiableContext<FeatureSchema, SchemaMapping> context();

    FeatureEventHandler<
            FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
        downstream();
  }

  static Decoder noop(String connector) {
    return new Decoder() {
      @Override
      public void decode(byte[] data, Pipeline pipeline) {
        // without a schema, the property is not selected, and the value is ignored downstream like
        // the value of any other column
        boolean isNotValue =
            pipeline.context().schema().filter(schema -> !schema.isValue()).isPresent();

        if (isNotValue) {
          throw new IllegalArgumentException(
              String.format("Only allowed in value mappings: [%s]", connector));
        }

        pipeline.downstream().onValue(pipeline.context());
      }

      @Override
      public void close() {}
    };
  }
}
