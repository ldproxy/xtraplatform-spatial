/**
 * Copyright 2020 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.provider;

import org.apache.felix.ipojo.ComponentInstance;
import org.apache.felix.ipojo.Factory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;

/**
 * @author zahnen
 */
public final class FactoryRegistryState<T> implements Registry.State<Factory>, FactoryRegistry<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FactoryRegistryState.class);

    private final String className;
    private final BundleContext bundleContext;
    private final Registry.State<Factory> factories;

    public FactoryRegistryState(String className, String shortName, BundleContext bundleContext,
                                String... componentProperties) {
        this.className = className;
        this.bundleContext = bundleContext;
        this.factories = new RegistryState<>(String.format("%s factory", shortName), bundleContext, componentProperties);
    }

    @Override
    public Optional<Factory> get(String... identifiers) {
        return factories.get(identifiers);
    }

    @Override
    public void onArrival(ServiceReference<Factory> ref) {
        factories.onArrival(ref);
    }

    @Override
    public void onDeparture(ServiceReference<Factory> ref) {
        factories.onDeparture(ref);
    }

    @Override
    public boolean ensureTypeExists() {
        try {
            bundleContext.getBundle()
                         .loadClass(className);
            return true;
        } catch (Throwable e) {
            LOGGER.error("Factory target class does not exist: {}", className);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Stacktrace", e);
            }
        }
        return false;
    }

    @Override
    public T createInstance(Map<String, Object> configuration, String... factoryProperties) {
        final Optional<Factory> factory = factories.get(factoryProperties);

        if (!factory.isPresent()) {
            throw new IllegalStateException();
        }

        try {
            ComponentInstance connectorInstance = factory.get()
                                                         .createComponentInstance(new Hashtable<>(configuration));
            ServiceReference<?>[] connectorRefs = bundleContext.getServiceReferences(className, "(instance.name=" + connectorInstance.getInstanceName() + ")");
            T connector = (T) bundleContext.getService(connectorRefs[0]);

            return connector;

        } catch (Throwable e) {
            throw new IllegalStateException("", e);
        }
    }
}
