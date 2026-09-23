/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import java.util.Optional;

/**
 * What every reference to a named result set has in common, whatever it compares: the name of the
 * set, the producing query that the service resolved it to, and the table the set was materialized
 * into. Lets the materializer collect, order and rewrite result sets without knowing whether a
 * reference compares a single value ({@link InResultSet}) or a composite key ({@link
 * InResultSetByKey}).
 */
public interface ResultSetReference {

  /** Name of the result set, as declared by the producing query. */
  String getSetName();

  /** Feature type of the query that defines the result set. */
  Optional<String> getProducerType();

  /** Effective filter of the query that defines the result set. */
  Optional<Cql2Expression> getProducerFilter();

  /** Name of the table the result set was materialized into, if it was too large to inline. */
  Optional<String> getMaterializedTable();
}
