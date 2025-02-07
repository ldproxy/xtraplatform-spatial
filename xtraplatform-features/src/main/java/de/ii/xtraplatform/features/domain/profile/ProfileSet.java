/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.profile;

import com.github.azahnen.dagger.annotations.AutoMultiBind;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import java.util.List;
import java.util.Optional;

@AutoMultiBind
public interface ProfileSet {

  /**
   * @return the prefix of the profile extension
   */
  String getPrefix();

  /**
   * @return the profile values of the profile extension
   */
  List<String> getValues();

  /**
   * @return an optional default value
   */
  default Optional<String> getDefault() {
    return Optional.empty();
  }

  /**
   * @param value the profile
   * @param schema the feature schema
   * @param mediaType the media type of t
   * @param builder
   */
  void addPropertyTransformations(
      String value,
      FeatureSchema schema,
      String mediaType,
      ImmutableProfileTransformations.Builder builder);
}
