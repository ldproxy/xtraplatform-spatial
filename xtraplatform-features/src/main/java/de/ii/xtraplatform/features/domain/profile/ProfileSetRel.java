/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.profile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.transform.FeatureRefResolver;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.core.MediaType;

public class ProfileSetRel implements ProfileSet {

  public static final String REL = "rel";
  public static final String AS_KEY = "rel-as-key";
  public static final String AS_URI = "rel-as-uri";
  public static final String AS_LINK = "rel-as-link";

  private static final String URI_TEMPLATE =
      String.format(
          "{{%s | orElse:'{{apiUri}}/collections/%s/items/%s'}}",
          FeatureRefResolver.URI_TEMPLATE, FeatureRefResolver.SUB_TYPE, FeatureRefResolver.SUB_ID);

  private static final String KEY_TEMPLATE =
      String.format(
          "{{%s | orElse:'%s::%s'}}",
          FeatureRefResolver.KEY_TEMPLATE, FeatureRefResolver.SUB_TYPE, FeatureRefResolver.SUB_ID);

  private static final String HTML_LINK_TEMPLATE =
      String.format("<a href=\"%s\">%s</a>", URI_TEMPLATE, FeatureRefResolver.SUB_TITLE);

  @Override
  public String getPrefix() {
    return REL;
  }

  @Override
  public List<String> getValues() {
    return List.of(AS_KEY, AS_URI, AS_LINK);
  }

  @Override
  public void addPropertyTransformations(
      String value,
      FeatureSchema schema,
      String mediaType,
      ImmutableProfileTransformations.Builder builder) {
    if (!getValues().contains(value)) {
      return;
    }

    schema.getAllNestedProperties().stream()
        .filter(SchemaBase::isFeatureRef)
        .forEach(
            property -> {
              switch (value) {
                case AS_KEY:
                  reduceToKey(property, builder);
                  break;
                case AS_URI:
                  reduceToUri(property, builder);
                  break;
                case AS_LINK:
                  if (mediaType.equals(MediaType.TEXT_HTML)) {
                    reduceToLink(property, builder);
                  } else {
                    mapToLink(property, builder);
                  }
                  break;
              }
            });
  }

  public static boolean usesFeatureRef(FeatureSchema featuretype) {
    return featuretype.getAllNestedProperties().stream().anyMatch(SchemaBase::isFeatureRef);
  }

  public static void reduceToKey(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            property
                        .getRefType()
                        .filter(
                            refType ->
                                !Objects.equals(refType, FeatureRefResolver.REF_TYPE_DYNAMIC))
                        .isPresent()
                    && property.getRefKeyTemplate().isEmpty()
                ? new ImmutablePropertyTransformation.Builder()
                    .objectRemoveSelect(FeatureRefResolver.ID)
                    .objectReduceSelect(FeatureRefResolver.ID)
                    .build()
                : new ImmutablePropertyTransformation.Builder()
                    .objectRemoveSelect(FeatureRefResolver.ID)
                    .objectReduceFormat(KEY_TEMPLATE)
                    .build()));
  }

  public static void reduceToUri(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectReduceFormat(URI_TEMPLATE)
                .build()));
  }

  public static void mapToLink(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectMapFormat(
                    ImmutableMap.of("title", FeatureRefResolver.SUB_TITLE, "href", URI_TEMPLATE))
                .build()));
  }

  public static void reduceToLink(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectReduceFormat(HTML_LINK_TEMPLATE)
                .build()));
  }
}
