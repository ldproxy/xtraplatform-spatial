/**
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.changes.sql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.features.domain.ExtensionConfiguration;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableFeatureChangesConfiguration.Builder.class)
public interface FeatureChangesConfiguration extends ExtensionConfiguration {

  List<String> getSubscribeToCollections();

  abstract class Builder extends ExtensionConfiguration.Builder {
  }

  @Override
  default Builder getBuilder() {
    return new ImmutableFeatureChangesConfiguration.Builder();
  }
}
