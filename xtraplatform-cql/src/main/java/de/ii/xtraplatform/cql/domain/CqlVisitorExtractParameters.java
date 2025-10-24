/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import de.ii.xtraplatform.jsonschema.domain.JsonSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// Parameters are a CQL2-JSON extension used in stored queries; they must not appear in
// normal CQL2 expressions. This visitor extracts all parameters used in a CQL expression
// along with their JSON Schema definitions, either from a global parameter definition
// or from the parameter object itself.
public class CqlVisitorExtractParameters extends CqlVisitorBase<Map<String, JsonSchema>> {

  private final Map<String, JsonSchema> globalParameters;
  private final Map<String, JsonSchema> found;

  public CqlVisitorExtractParameters(Map<String, JsonSchema> globalParameters) {
    this.globalParameters = globalParameters;
    this.found = new HashMap<>();
  }

  @Override
  public Map<String, JsonSchema> visit(
      Parameter parameter, List<Map<String, JsonSchema>> children) {
    String name = parameter.getName();
    JsonSchema schema =
        Objects.requireNonNullElse(globalParameters.get(name), parameter.getSchema());
    found.put(name, schema);
    return null;
  }

  @Override
  public Map<String, JsonSchema> postProcess(CqlNode node, Map<String, JsonSchema> parameters) {
    Map<String, JsonSchema> result = Map.copyOf(found);
    found.clear();
    return result;
  }
}
