/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorCopy;
import de.ii.xtraplatform.cql.domain.Property;
import java.util.List;

public class CqlVisitorPropertyPrefix extends CqlVisitorCopy {

  private final String prefix;

  public CqlVisitorPropertyPrefix(String prefix) {
    this.prefix = prefix + ".";
  }

  @Override
  public CqlNode visit(Property property, List<CqlNode> children) {
    // strip double quotes from the property name
    String propertyName = property.getName().replaceAll("^\"|\"$", "");
    if (!propertyName.startsWith(prefix)) {
      return Property.of(prefix + propertyName, property.getNestedFilters());
    }
    return property;
  }
}
