/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new", deepImmutablesDetection = true)
@JsonDeserialize(builder = ImmutableFeatureTokens.Builder.class)
public interface FeatureTokens {

  List<FeatureToken> getTokens();

  default List<Object> asSource() {
    return asTokens(true, null);
  }

  default List<Object> asSource(String onlyIf) {
    return asTokens(true, onlyIf);
  }

  default List<Object> asTarget() {
    return asTokens(false, null);
  }

  default List<Object> asTarget(String onlyIf) {
    return asTokens(false, onlyIf);
  }

  Splitter SOURCE_SPLITTER = Splitter.on('/').omitEmptyStrings();

  Splitter TARGET_SPLITTER = Splitter.on('.').omitEmptyStrings();

  default List<Object> asTokens(boolean isSource, String onlyIf) {
    List<Object> tokens = new ArrayList<>();

    for (FeatureToken token : getTokens()) {
      if (skip(token.getOnlyIf(), onlyIf)) {
        continue;
      }

      tokens.add(token.getType());

      if (isSource && Objects.nonNull(token.getSource())) {
        tokens.add(SOURCE_SPLITTER.splitToList(token.getSource()));
      }

      if (!isSource) {
        tokens.add(TARGET_SPLITTER.splitToList(token.getTarget()));
      }

      if (Objects.nonNull(token.getValue())) {
        tokens.add(token.getValue());
      } else if (token.getType() == FeatureTokenType.VALUE) {
        tokens.add(null);
      }

      if (Objects.nonNull(token.getValueType())) {
        tokens.add(token.getValueType());
      }

      if (Objects.nonNull(token.getGeometry())) {
        tokens.add(token.getGeometry());
      }
    }

    return tokens;
  }

  Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  static boolean skip(String onlyIf, String filter) {
    if (Objects.isNull(onlyIf) || Objects.isNull(filter)) {
      return false;
    }
    if (!onlyIf.contains(",")) {
      return !Objects.equals(filter, onlyIf);
    }

    return SPLITTER.splitToStream(onlyIf).noneMatch(o -> Objects.equals(o, filter));
  }
}
