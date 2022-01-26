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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;
import org.threeten.extra.Interval;

import java.util.List;
import java.util.Objects;

@Value.Immutable
@JsonDeserialize(builder = ImmutableTContains.Builder.class)
public interface TContains extends TemporalOperation, CqlNode {

    @JsonCreator
    static TContains of(List<Operand> operands) {
        return new ImmutableTContains.Builder().operands(operands)
                .build();
    }

    static TContains of(String property, TemporalLiteral temporalLiteral) {
        if (!Objects.equals(temporalLiteral.getType(), ImmutableCqlInterval.class)) {
            throw new IllegalArgumentException(String.format("not a valid interval: %s", temporalLiteral));
        }
        return new ImmutableTContains.Builder().operands(ImmutableList.of(Property.of(property), temporalLiteral))
                .build();
    }

    static TContains of(String property, String property2) {
        return new ImmutableTContains.Builder().operands(ImmutableList.of(Property.of(property), Property.of(property2)))
                .build();
    }

    abstract class Builder extends TemporalOperation.Builder<TContains> {
    }

    @Value.Check
    @Override
    default void check() {
        TemporalOperation.super.check();
        Preconditions.checkState( getOperands().get(0) instanceof Property ||
                        (getOperands().get(0) instanceof TemporalLiteral &&
                                Objects.equals(((TemporalLiteral) getOperands().get(0)).getType(), Interval.class)),
                "The first argument of T_CONTAINS must be a property or a time interval, found %s",
                getOperands().get(0) instanceof Property
                        ? ((Property) getOperands().get(0)).getName()
                        : ((TemporalLiteral) getOperands().get(0)).getValue());
        Preconditions.checkState( getOperands().get(0) instanceof Property ||
                        (getOperands().get(1) instanceof TemporalLiteral &&
                                Objects.equals(((TemporalLiteral) getOperands().get(1)).getType(), Interval.class)),
                "The second argument of T_CONTAINS must be a property or a time interval, found %s",
                getOperands().get(1) instanceof Property
                        ? ((Property) getOperands().get(1)).getName()
                        : ((TemporalLiteral) getOperands().get(1)).getValue());
    }

}
