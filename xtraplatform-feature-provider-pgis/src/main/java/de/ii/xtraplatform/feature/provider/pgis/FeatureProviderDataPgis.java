/**
 * Copyright 2019 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider.pgis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.feature.provider.api.TargetMapping;
import de.ii.xtraplatform.feature.provider.sql.SqlConnectorSlick;
import de.ii.xtraplatform.feature.transformer.api.FeatureProviderDataTransformer;
import de.ii.xtraplatform.feature.transformer.api.ImmutableMappingStatus;
import de.ii.xtraplatform.feature.transformer.api.MappingStatus;
import org.immutables.value.Value;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * @author zahnen
 */
@Value.Immutable
@Value.Modifiable
@JsonDeserialize(as = ModifiableFeatureProviderDataPgis.class)
public abstract class FeatureProviderDataPgis extends FeatureProviderDataTransformer {

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
    @Value.Default
    @Override
    public String getProviderType() {
        return FeatureProviderPgis.PROVIDER_TYPE;
    }

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
    @Value.Default
    @Override
    public String getConnectorType() {
        return SqlConnectorSlick.CONNECTOR_TYPE;
    }

    public abstract ConnectionInfo getConnectionInfo();

    @Value.Default
    @Override
    public MappingStatus getMappingStatus() {
        return new ImmutableMappingStatus.Builder()
                                     .enabled(true)
                                     .supported(true)
                                     .build();
    }

    @Value.Default
    public boolean computeNumberMatched() {
        return true;
    }

    @Nullable
    public abstract FeatureActionTrigger getTrigger();

    @Override
    public boolean isFeatureTypeEnabled(String featureType) {
        //TODO: better way to detect mapping for feature type
        // returns only enabled mappings
        final Optional<TargetMapping> mapping = getMappings()
                .get(featureType)
                .findMappings("/" + featureType, TargetMapping.BASE_TYPE);
        return /*!service.getServiceProperties()
                           .getMappingStatus()
                           .isEnabled() ||*/ mapping.isPresent();
    }

    @Override
    public boolean supportsTransactions() {
        return true;
    }
}
