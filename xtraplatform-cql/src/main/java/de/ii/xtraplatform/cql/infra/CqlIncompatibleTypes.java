/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.infra;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class CqlIncompatibleTypes extends IllegalArgumentException {
  public CqlIncompatibleTypes(String cqlText, String type, List<String> expectedTypes) {
    super(
        String.format(
            "Incompatible types in CQL2 filter. Found type '%s' in expression [%s]. Valid types: %s",
            type, cqlText, String.join(", ", expectedTypes)));
  }

  public CqlIncompatibleTypes(String cqlText, List<String> types, Set<Set<String>> expectedTypes) {
    super(
        String.format(
            "Incompatible types in CQL2 filter. Expression \"%s\" has types: %s. Valid type combinations: %s",
            cqlText,
            String.join(", ", types),
            expectedTypes.stream()
                .map(typeList -> String.join(", ", typeList))
                .collect(Collectors.joining(" or "))));
  }

  public CqlIncompatibleTypes(String cqlText, int i, String type, List<String> expectedTypes) {
    super(
        String.format(
            "Incompatible types in CQL2 filter. Argument %d in function \"%s\" has type %s. Valid types: %s",
            i, cqlText, type, String.join(", ", expectedTypes)));
  }
}
