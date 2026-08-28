/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.Collection;
import java.util.Properties;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/**
 * An {@link SSLSocketFactory} that reads the private key and certificates from PEM files, and wraps
 * them in a PKCS#12 keystore for the JCE. This is necessary because the default factory of the
 * PostgreSQL JDBC driver does not support all formats of PEM files directly, specifically PKCS#1
 * and SEC1.
 */
public final class PkcsAwareSslSocketFactory extends SSLSocketFactory {

  private final SSLSocketFactory delegate;

  public PkcsAwareSslSocketFactory(Properties info) throws GeneralSecurityException, IOException {
    Path keyFile = requiredPath(info, "sslkey");
    Path certificateFile = requiredPath(info, "sslcert");
    String rootCertificate = info.getProperty("sslrootcert");

    SSLContext context = SSLContext.getInstance("TLS");
    context.init(
        keyManagers(keyFile, certificateFile),
        rootCertificate == null ? null : trustManagers(Path.of(rootCertificate)),
        null);
    this.delegate = context.getSocketFactory();
  }

  private static KeyManager[] keyManagers(Path keyFile, Path certificateFile)
      throws GeneralSecurityException, IOException {
    PrivateKey key = PkcsAwarePrivateKeyReader.read(keyFile);

    char[] password = ephemeralPassword();
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry("client", key, password, readCertificates(certificateFile));

    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keyStore, password);
    return keyManagers.getKeyManagers();
  }

  private static TrustManager[] trustManagers(Path rootCertificateFile)
      throws GeneralSecurityException, IOException {
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null);
    int index = 0;
    for (Certificate certificate : readCertificates(rootCertificateFile)) {
      trustStore.setCertificateEntry("ca-" + index++, certificate);
    }

    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trustStore);
    return trustManagers.getTrustManagers();
  }

  private static Certificate[] readCertificates(Path file)
      throws GeneralSecurityException, IOException {
    try (InputStream in = Files.newInputStream(file)) {
      Collection<? extends Certificate> certificates =
          CertificateFactory.getInstance("X.509").generateCertificates(in);
      if (certificates.isEmpty()) {
        throw new IllegalArgumentException("%s contains no X.509 certificate".formatted(file));
      }
      return certificates.toArray(Certificate[]::new);
    }
  }

  private static Path requiredPath(Properties info, String property) {
    String value = info.getProperty(property);
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(
          "connection property \"%s\" is required".formatted(property));
    }
    Path path = Path.of(value);
    if (!Files.isReadable(path)) {
      throw new IllegalArgumentException(
          "\"%s\" points at %s, which is not readable".formatted(property, path));
    }
    return path;
  }

  private static char[] ephemeralPassword() {
    byte[] random = new byte[32];
    new SecureRandom().nextBytes(random);
    return Base64.getEncoder().encodeToString(random).toCharArray();
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return delegate.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return delegate.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
      throws IOException {
    return delegate.createSocket(socket, host, port, autoClose);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return delegate.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return delegate.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return delegate.createSocket(host, port);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return delegate.createSocket(address, port, localAddress, localPort);
  }
}
