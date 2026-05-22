/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureStoreInstanceContainer;
import de.ii.xtraplatform.features.domain.FeatureStorePathParser;
import de.ii.xtraplatform.features.domain.ImmutableFeatureStoreInstanceContainer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class FeatureStorePathParserWfs implements FeatureStorePathParser {

  public FeatureStorePathParserWfs(Map<String, String> namespaces) {
    Objects.requireNonNull(namespaces);
  }

  @Override
  public List<FeatureStoreInstanceContainer> parse(FeatureSchema schema) {

    LinkedHashMap<String, ImmutableFeatureStoreInstanceContainer.Builder>
        instanceContainerBuilders = new LinkedHashMap<>();

    String instanceContainerName = schema.getName();

    /*List<String> path = schema.getProperties()
    .stream()
     .filter(property -> property.getSourcePath().isPresent())
    .findFirst()
    .map(property -> property.getSourcePath().get().substring(1, property.getSourcePath().get().indexOf("/", 1)))
    .map(ImmutableList::of)
    .orElse(ImmutableList.of());*/

    instanceContainerBuilders.put(
        instanceContainerName, ImmutableFeatureStoreInstanceContainer.builder());

    /*ImmutableFeatureStoreAttribute attribute = ImmutableFeatureStoreAttribute.builder()
                                                                                 .name("id")
                                                                                 .path(ImmutableList.of("http://repository.gdi-de.org/schemas/adv/produkt/alkis-vereinfacht/2.0:Flurstueck"))
                                                                                 .addPath("id")
                                                                                 .queryable("id")
                                                                                 .isId(true)
                                                                                 .isSpatial(false)
                                                                                 .build();
    */
    instanceContainerBuilders
        .get(instanceContainerName)
        .name(instanceContainerName)
        // TODO.path(path)
        .sortKey("none")
        // .attributes(ImmutableList.of(attribute))
        .attributesPosition(0);

    return instanceContainerBuilders.values().stream()
        .map(ImmutableFeatureStoreInstanceContainer.Builder::build)
        .collect(Collectors.toList());
  }
}
