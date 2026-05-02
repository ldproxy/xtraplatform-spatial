/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.ImmutableJacksonSubType;
import de.ii.xtraplatform.base.domain.JacksonSubTypeIds;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import de.ii.xtraplatform.features.graphql.domain.ConnectionInfoGraphQlHttp;
import de.ii.xtraplatform.features.graphql.infra.GraphQlConnectorHttp;
import java.util.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * @author zahnen
 */
@Singleton
@AutoBind
public class JacksonSubTypeIdsGraphQl implements JacksonSubTypeIds {

  @Inject
  public JacksonSubTypeIdsGraphQl() {}

  @Override
  public List<JacksonSubType> getSubTypes() {
    return ImmutableList.of(
        ImmutableJacksonSubType.builder()
            .superType(ConnectionInfo.class)
            .subType(ConnectionInfoGraphQlHttp.class)
            .id(GraphQlConnectorHttp.CONNECTOR_TYPE)
            .build());
  }
}
