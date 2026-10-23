/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

class JobHookSpec extends Specification {

    def 'the default methods are inert'() {

        given:

        JobHook hook = new JobHook() {}

        when:

        hook.init(100)
        hook.update(1)
        hook.checkpoint()

        then:

        noExceptionThrown()
        !hook.isCancelRequested()

    }

    def 'checkpoint throws when cancellation was requested'() {

        given:

        JobHook hook = new JobHook() {
            @Override
            boolean isCancelRequested() {
                return true
            }
        }

        when:

        hook.checkpoint()

        then:

        thrown(JobCancelledException)

    }

    def 'checkpoint passes when cancellation was not requested'() {

        given:

        JobHook hook = new JobHook() {}

        when:

        hook.checkpoint()

        then:

        noExceptionThrown()

    }
}
