/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableLt.Builder.class)
public interface Lt extends BinaryScalarOperation, CqlNode {

    static Lt of(String property, ScalarLiteral scalarLiteral) {
        return new ImmutableLt.Builder().operand1(Property.of(property))
                                         .operand2(scalarLiteral)
                                         .build();
    }

    static Lt of(String property, String property2) {
        return new ImmutableLt.Builder().operand1(Property.of(property))
                                         .operand2(Property.of(property2))
                                         .build();
    }

    static Lt of(Property property, ScalarLiteral scalarLiteral) {
        return new ImmutableLt.Builder().operand1(property)
                                         .operand2(scalarLiteral)
                                         .build();
    }

    static Lt of(Property property, Property property2) {
        return new ImmutableLt.Builder().operand1(property)
                                         .operand2(property2)
                                         .build();
    }

    static Lt ofFunction(Function function, ScalarLiteral scalarLiteral) {
        return new ImmutableLt.Builder().operand1(function)
                                         .operand2(scalarLiteral)
                                         .build();
    }

    abstract class Builder extends BinaryScalarOperation.Builder<Lt> {
    }

}
