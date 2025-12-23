/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface MultiFeatureQuery extends Query {

  @Value.Immutable
  interface SubQuery extends TypeQuery {
    String getCollectionId();
  }

  List<SubQuery> getQueries();
}
