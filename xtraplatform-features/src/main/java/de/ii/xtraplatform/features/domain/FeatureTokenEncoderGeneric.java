/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.streams.domain.Reactive.TransformerCustomFuseableIn;

public interface FeatureTokenEncoderGeneric<
        T extends SchemaBase<T>, U extends SchemaMappingBase<T>, V extends ModifiableContext<T, U>>
    extends TransformerCustomFuseableIn<Object, byte[], FeatureEventHandler<T, U, V>>,
        // NOTE: TransformerCustomSink<Object, byte[], FeatureTokenSinkReduced<?>>,
        FeatureEventHandler<T, U, V>,
        FeatureTokenContext<V> {}
