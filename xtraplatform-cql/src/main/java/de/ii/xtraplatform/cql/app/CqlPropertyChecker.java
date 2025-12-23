/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import static de.ii.xtraplatform.cql.domain.In.ID_PLACEHOLDER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitorBase;
import de.ii.xtraplatform.cql.domain.Property;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CqlPropertyChecker extends CqlVisitorBase<List<String>> {

  private final Collection<String> allowedProperties;
  private final List<String> invalidProperties = new ArrayList<>();

  public CqlPropertyChecker(Collection<String> allowedProperties) {
    this.allowedProperties =
        ImmutableList.<String>builder().addAll(allowedProperties).add(ID_PLACEHOLDER).build();
  }

  @Override
  public List<String> postProcess(CqlNode node, List<String> strings) {
    ImmutableList<String> result = ImmutableList.copyOf(invalidProperties);
    invalidProperties.clear();
    return result;
  }

  @Override
  public List<String> visit(Property property, List<List<String>> children) {
    // strip double quotes
    String propertyName = property.getName().replaceAll("^\"|\"$", "");
    if (!allowedProperties.contains(propertyName)) {
      invalidProperties.add(propertyName);
    }
    return Lists.newArrayList();
  }
}
