/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.ImmutableJacksonSubType;
import de.ii.xtraplatform.base.domain.JacksonSubTypeIds;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.sql.domain.ConnectionInfoSql;
import de.ii.xtraplatform.features.sql.infra.db.SqlConnectorRx;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;

/**
 * @author zahnen
 */
@Singleton
@AutoBind
public class FeatureProviderRegisterSql implements JacksonSubTypeIds {

  @Inject
  public FeatureProviderRegisterSql() {}

  @Override
  public List<JacksonSubType> getSubTypes() {
    return ImmutableList.of(
        ImmutableJacksonSubType.builder()
            .superType(ConnectionInfo.class)
            .subType(ConnectionInfoSql.class)
            .id(SqlConnectorRx.CONNECTOR_TYPE)
            .build());
  }
}
