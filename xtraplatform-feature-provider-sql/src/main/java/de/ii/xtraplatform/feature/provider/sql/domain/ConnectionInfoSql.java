/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider.sql.domain;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.features.domain.ConnectionInfo;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * @author zahnen
 */
@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableConnectionInfoSql.Builder.class)
public interface ConnectionInfoSql extends ConnectionInfo {

    enum Dialect {PGIS,GPKG}

    Optional<String> getHost();

    String getDatabase();

    Optional<String> getUser();

    Optional<String> getPassword();

    List<String> getSchemas();

    @Value.Default
    default Dialect getDialect() {
        return Dialect.PGIS;
    }

    @Value.Default
    default boolean getComputeNumberMatched() {
        return true;
    }

    @JsonAlias("pathSyntax")
    @Value.Default
    default SqlPathDefaults getSourcePathDefaults() {
        return new ImmutableSqlPathDefaults.Builder().build();
    }

    Optional<FeatureActionTrigger> getTriggers();

    @JsonAlias("maxThreads")
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
    @Value.Default
    default int getMaxConnections() {
        return -1;
    }

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
    @Value.Default
    default int getMinConnections() {
        return getMaxConnections();
    }

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY) // means only read from json
    @Value.Default
    default boolean getInitFailFast() {
        return true;
    }
}
