/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.ii.xtraplatform.docs.DocIgnore;
import java.util.Optional;

/**
 * @lang_en
 * @de
 * @default
 */
public interface ConnectionInfo {

  @DocIgnore
  Optional<String> getConnectionUri();

  @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
  String getConnectorType();

  @JsonIgnore
  default boolean isShared() {
    return false;
  }

  String getDatasetIdentifier();
}
