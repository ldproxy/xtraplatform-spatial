/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.pipeline;

import de.ii.xtraplatform.streams.domain.Reactive.TransformerCustomFuseableIn;

public interface FeatureTokenEncoderGenericSimple<
        T, U, V extends FeatureEventHandlerSimple.ModifiableContext<T, U>, W>
    extends TransformerCustomFuseableIn<Object, W, FeatureEventHandlerSimple<T, U, V>>,
        FeatureEventHandlerSimple<T, U, V>,
        FeatureTokenContextSimple<V> {}
