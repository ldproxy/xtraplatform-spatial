/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface FeatureStoreInsertGenerator {
  Tuple<String, Consumer<String>> createInsert(
      FeatureDataSql feature, List<Integer> parentRows, Optional<String> id, EpsgCrs crs);

  Tuple<String, Consumer<String>> createJunctionInsert(
      FeatureDataSql feature, List<Integer> parentRows);

  Tuple<String, Consumer<String>> createForeignKeyUpdate(
      FeatureDataSql feature, List<Integer> parentRows);
}
