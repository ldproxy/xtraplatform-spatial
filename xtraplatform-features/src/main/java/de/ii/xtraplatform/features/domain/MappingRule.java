/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import java.util.Optional;
import java.util.regex.Pattern;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(
    builder = "new",
    get = {"is*", "get*"}) // , deepImmutablesDetection = true, attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableMappingRule.Builder.class)
public interface MappingRule {
  String getSource();

  String getTarget();

  SchemaBase.Type getType();

  Optional<SchemaBase.Role> getRole();

  int getIndex();

  @JsonIgnore
  @Value.Lazy
  default String getIdentifier() {
    return getSource() + "-" + getTarget();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean hasSourceParent() {
    return maskPathAttributes(getSource()).contains("/");
  }

  @JsonIgnore
  @Value.Lazy
  default String getSourceParent() {
    return hasSourceParent()
        ? getSource().substring(0, maskPathAttributes(getSource()).lastIndexOf("/"))
        : "";
  }

  @JsonIgnore
  @Value.Lazy
  default String getTargetParent() {
    return getTarget().contains(".") ? getTarget().substring(0, getTarget().lastIndexOf(".")) : "$";
  }

  @JsonIgnore
  @Value.Lazy
  default String getIdentifierParent() {
    return getSourceParent() + "-" + getTargetParent();
  }

  @JsonIgnore
  @Value.Lazy
  default boolean isObjectOrArray() {
    return getType() == Type.OBJECT_ARRAY
        || getType() == Type.OBJECT
        || getType() == Type.VALUE_ARRAY;
  }

  static String maskPathAttributes(String path) {
    return Pattern.compile("\\{([^}]*)\\}")
        .matcher(path)
        .replaceAll(m -> "{" + "_".repeat(m.group(1).length()) + "}");
  }
}
