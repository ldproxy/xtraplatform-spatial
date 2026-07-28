/*
 * Copyright 2024 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.Lazy;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapter;
import de.ii.xtraplatform.features.sql.domain.SqlDbmsAdapters;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Objects;
import java.util.Set;

@Singleton
@AutoBind
public class SqlDbmsAdaptersImpl implements SqlDbmsAdapters {

  private final Lazy<Set<SqlDbmsAdapter>> dbmsAdapters;

  @Inject
  public SqlDbmsAdaptersImpl(Lazy<Set<SqlDbmsAdapter>> dbmsAdapters) {
    this.dbmsAdapters = dbmsAdapters;
  }

  private static boolean isSameDbms(String dbmsId, String dbmsId2) {
    return Objects.equals(dbmsId, dbmsId2);
  }

  @Override
  public boolean isSupported(String dbmsId) {
    return dbmsAdapters.get().stream().anyMatch(factory -> isSameDbms(factory.getId(), dbmsId));
  }

  @Override
  public SqlDbmsAdapter get(String dbmsId) {
    return dbmsAdapters.get().stream()
        .filter(factory -> isSameDbms(factory.getId(), dbmsId))
        .findFirst()
        .orElseThrow();
  }

  @Override
  public SqlDialect getDialect(String dbmsId) {
    return get(dbmsId).getDialect();
  }
}
