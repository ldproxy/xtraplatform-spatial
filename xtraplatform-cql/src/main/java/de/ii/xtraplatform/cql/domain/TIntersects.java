/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

import java.util.List;

@Value.Immutable
@JsonDeserialize(as = TIntersects.class)
public interface TIntersects extends TemporalOperation, CqlNode {

    @JsonCreator
    static TIntersects of(List<Operand> operands) {
        return new ImmutableTIntersects.Builder().operands(operands)
                .build();
    }

    static TIntersects of(Temporal temporal1, Temporal temporal2) {
        return new ImmutableTIntersects.Builder().operands(ImmutableList.of(temporal1,temporal2))
                .build();
    }

    static TIntersects of(String property, TemporalLiteral temporalLiteral) {
        return new ImmutableTIntersects.Builder().operands(ImmutableList.of(Property.of(property),temporalLiteral))
                .build();
    }

    static TIntersects of(String property, String property2) {
        return new ImmutableTIntersects.Builder().operands(ImmutableList.of(Property.of(property), Property.of(property2)))
                .build();
    }

    abstract class Builder extends TemporalOperation.Builder<TIntersects> {
    }

}
