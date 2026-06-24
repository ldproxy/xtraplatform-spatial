/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

/**
 * A {@link FeatureQueryExtension} that contributes a {@link FeatureTokenTransformer} to the feature
 * stream pipeline. Attached to a {@link FeatureQuery} by upstream code (e.g. a profile's {@code
 * transformFeatureQuery}); {@link FeatureStreamImpl} discovers all such extensions on the query and
 * wires their transformers in immediately after {@code FeatureTokenTransformerPropertyLinks},
 * before the per-format value-transformation step. This is the right slot for transformers that
 * need to see raw provider values (pre-format) and rewrite tokens in-place.
 */
public interface FeatureTokenTransformerExtension extends FeatureQueryExtension {

  FeatureTokenTransformer createTransformer();
}
