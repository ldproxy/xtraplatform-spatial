/**
 * Copyright 2021 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import org.immutables.value.Value;

import java.util.List;

public interface NonBinaryScalarOperation extends CqlNode {

    @JsonIgnore
    List<Operand> getOperands();

    @Value.Check
    default void check() {
        getOperands().stream()
                     .forEach(operand -> Preconditions.checkState(operand instanceof Scalar, "a scalar operation must have scalar operands, found %s", operand.getClass().getSimpleName()));
   }

    abstract class Builder<T extends NonBinaryScalarOperation> {}
}
