/**
 * Copyright 2020 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.app;

import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.entity.api.EntityData;
import de.ii.xtraplatform.event.store.EntityDataBuilder;
import de.ii.xtraplatform.event.store.EntityMigration;
import de.ii.xtraplatform.event.store.Identifier;
import de.ii.xtraplatform.features.domain.FeatureProperty;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV1;
import de.ii.xtraplatform.features.domain.FeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.FeatureType;
import de.ii.xtraplatform.features.domain.ImmutableFeatureProviderDataV1;
import de.ii.xtraplatform.features.domain.ImmutableFeatureProviderDataV2;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class FeatureProviderDataMigrationV1V2 implements EntityMigration<FeatureProviderDataV1, FeatureProviderDataV2> {
    @Override
    public long getSourceVersion() {
        return 1;
    }

    @Override
    public long getTargetVersion() {
        return 2;
    }

    @Override
    public EntityDataBuilder<FeatureProviderDataV1> getDataBuilder() {
        return new ImmutableFeatureProviderDataV1.Builder();
    }

    @Override
    public FeatureProviderDataV2 migrate(FeatureProviderDataV1 entityData) {

        ImmutableFeatureProviderDataV2.Builder builder = new ImmutableFeatureProviderDataV2.Builder()
                .id(entityData.getId())
                .createdAt(entityData.getCreatedAt())
                .lastModified(entityData.getLastModified())
                .providerType(entityData.getProviderType())
                .featureProviderType(entityData.getFeatureProviderType())
                .nativeCrs(entityData.getNativeCrs());

        entityData.getTypes()
                  .values()
                  .forEach(featureType -> {
                      FeatureSchema featureTypeNew = migrateFeatureType(featureType);

                      builder.putTypes(featureTypeNew.getName(), featureTypeNew);

                  });

        return builder.build();
    }

    @Override
    public Map<Identifier, EntityData> getAdditionalEntities(Identifier identifier, FeatureProviderDataV1 entityData) {
        return ImmutableMap.of();
    }

    public FeatureSchema migrateFeatureType(FeatureType featureType) {

        String featureTypePath = getFeatureTypePath(featureType);

        return new ImmutableFeatureSchema.Builder()
                .name(featureType.getName())
                .type(SchemaBase.Type.OBJECT)
                .sourcePath(featureTypePath)
                .setPropertyMap(migrateProperties(featureType.getProperties(), featureTypePath))
                .build();
    }

    private Map<String, ImmutableFeatureSchema.Builder> migrateProperties(Map<String, FeatureProperty> properties,
                                                                              String featureTypePath) {
        Map<String, ImmutableFeatureSchema.Builder> migrated = new LinkedHashMap<>();

        Map<String, ImmutableFeatureSchema.Builder> objectPropertiesCache = new LinkedHashMap<>();

        properties.entrySet()
                  .stream()
                  .map(entry -> {

                      String[] nameTokens = entry.getKey()
                                                 .split("\\.");
                      FeatureProperty originalProperty = entry.getValue();
                      String path = removePrefix(originalProperty.getPath(), featureTypePath);
                      FeatureSchema.Type type = FeatureSchema.Type.valueOf(originalProperty.getType()
                                                                                           .toString());
                      Optional<FeatureSchema.Role> role = originalProperty.getRole()
                                                                          .map(originalRole -> FeatureSchema.Role.valueOf(originalRole.toString()));

                      return createPropertyTree(objectPropertiesCache, nameTokens, path, type, role);

                  })
                  .forEach(entry -> migrated.put(entry.getKey(), entry.getValue()));

        return migrated;
    }

    private Map.Entry<String, ImmutableFeatureSchema.Builder> createPropertyTree(
            Map<String, ImmutableFeatureSchema.Builder> objectPropertiesCache,
            String[] nameTokens,
            String path,
            FeatureSchema.Type type,
            Optional<FeatureSchema.Role> role) {

        String name = nameTokens[0];

        if (nameTokens.length == 1) {
            return new AbstractMap.SimpleImmutableEntry<>(clean(name), getValueProperty(name, path, type, role));
        }

        String objectPath = isNamedArray(name)
                ? removeSuffix(path, getArrayName(name))
                : removeSuffix(path, isArray(nameTokens[1]) ? 2 : 1);
        String childPath = removePrefix(path, objectPath);

        ImmutableFeatureSchema.Builder objectProperty = objectPropertiesCache.computeIfAbsent(clean(name), cleanName -> getObjectProperty(name, objectPath));

        Map.Entry<String, ImmutableFeatureSchema.Builder> childTree = createPropertyTree(objectPropertiesCache, Arrays.copyOfRange(nameTokens, 1, nameTokens.length), childPath, type, role);

        objectProperty.putPropertyMap(childTree.getKey(), childTree.getValue());

        return new AbstractMap.SimpleImmutableEntry<>(clean(name), objectProperty);

    }

    private ImmutableFeatureSchema.Builder getValueProperty(String name, String path, FeatureSchema.Type type,
                                                                Optional<FeatureSchema.Role> role) {

        ImmutableFeatureSchema.Builder builder = new ImmutableFeatureSchema.Builder()
                .name(clean(name))
                .sourcePath(path)
                .type(type)
                .role(role);

        if (isArray(name)) {
            builder.type(FeatureSchema.Type.VALUE_ARRAY)
                   .valueType(type);
        }

        return builder;
    }

    private ImmutableFeatureSchema.Builder getObjectProperty(String name, String path) {
        return new ImmutableFeatureSchema.Builder()
                .name(clean(name))
                .sourcePath(path)
                .type(isArray(name) ? FeatureSchema.Type.OBJECT_ARRAY : FeatureSchema.Type.OBJECT);
    }

    private String getFeatureTypePath(FeatureType featureType) {
        String firstPathElement = featureType.getPropertiesByPath()
                                             .keySet()
                                             .stream()
                                             .map(property -> property.get(0))
                                             .findFirst()
                                             .orElse("");
        return "/" + firstPathElement;
    }

    private String removePrefix(String path, String prefix) {
        return path.substring(prefix.length() + 1);
    }

    private String removeSuffix(String path, int numberOfElements) {
        int slashPos = path.length();
        for (int i = 0; i < numberOfElements; i++) {
            slashPos = path.lastIndexOf("/", slashPos - 1);
        }
        int end = slashPos > -1 ? slashPos : 0;

        return path.substring(0, end);
    }

    private String removeSuffix(String path, String afterElement) {
        int elementPos = path.lastIndexOf("]" + afterElement + "/");
        int end = elementPos > -1 ? path.indexOf("/", elementPos) : 0;

        return path.substring(0, end);
    }

    private int squareBracketStart(String name) {
        return name.indexOf("[");
    }

    private int squareBracketEnd(String name) {
        return name.indexOf("]");
    }

    private boolean isArray(String name) {
        return squareBracketStart(name) > 0;
    }

    private boolean isNamedArray(String name) {
        int start = squareBracketStart(name);
        return start > 0 && squareBracketEnd(name) > start + 1;
    }

    private String getArrayName(String name) {
        if (isNamedArray(name)) {
            return name.substring(squareBracketStart(name) + 1, squareBracketEnd(name));
        }
        return "";
    }

    private String clean(String name) {
        if (isArray(name)) {
            return name.substring(0, squareBracketStart(name));
        }

        return name;
    }
}
