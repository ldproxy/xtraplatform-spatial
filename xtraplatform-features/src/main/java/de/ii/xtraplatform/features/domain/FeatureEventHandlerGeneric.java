/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureEventHandlerGeneric.GenericContext;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;

// NOTE: more comfortable variant of consumer, use in encoder/transformer
public interface FeatureEventHandlerGeneric
    extends FeatureEventHandler<
            FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>,
        FeatureTokenContext<GenericContext> {

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface GenericContext extends ModifiableContext<FeatureSchema, SchemaMapping> {}

  @Override
  default GenericContext createContext() {
    return ModifiableGenericContext.create();
  }
}
