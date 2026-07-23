/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.features.domain.FeatureStream.ResultBase;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import de.ii.xtraplatform.streams.domain.Reactive.Stream;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

@FunctionalInterface
public interface QueryRunner {
  <W extends ResultBase> CompletionStage<W> runQuery(
      BiFunction<FeatureTokenSource, Map<String, String>, Stream<W>> stream,
      Query query,
      Map<String, PropertyTransformations> propertyTransformations,
      boolean passThrough);
}
