/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import com.github.azahnen.dagger.annotations.AutoBind;
import dagger.Lazy;
import de.ii.xtraplatform.features.domain.FeatureQueriesExtension;
import de.ii.xtraplatform.features.domain.ProviderExtensionRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Optional;
import java.util.Set;

@Singleton
@AutoBind
public class ProviderExtensionRegistryImpl implements ProviderExtensionRegistry {

  private final Lazy<Set<FeatureQueriesExtension>> extensions;

  @Inject
  public ProviderExtensionRegistryImpl(Lazy<Set<FeatureQueriesExtension>> extensions) {
    this.extensions = extensions;
  }

  @Override
  public Set<FeatureQueriesExtension> getAll() {
    return extensions.get();
  }

  @Override
  public Optional<FeatureQueriesExtension> get(String... identifiers) {
    return Optional.empty();
  }
}
