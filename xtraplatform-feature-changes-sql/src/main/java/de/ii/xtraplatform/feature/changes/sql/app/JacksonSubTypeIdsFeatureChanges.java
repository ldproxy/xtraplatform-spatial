/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.base.domain.ImmutableJacksonSubType;
import de.ii.xtraplatform.base.domain.JacksonSubTypeIds;
import de.ii.xtraplatform.feature.changes.sql.domain.FeatureChangesPgConfiguration;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import java.util.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@AutoBind
public class JacksonSubTypeIdsFeatureChanges implements JacksonSubTypeIds {

  @Inject
  public JacksonSubTypeIdsFeatureChanges() {}

  @Override
  public List<JacksonSubType> getSubTypes() {
    return ImmutableList.of(
        ImmutableJacksonSubType.builder()
            .superType(ExtensionConfiguration.class)
            .subType(FeatureChangesPgConfiguration.class)
            .id(ExtensionConfiguration.getIdentifier(FeatureChangesPgConfiguration.class))
            .build());
  }
}
