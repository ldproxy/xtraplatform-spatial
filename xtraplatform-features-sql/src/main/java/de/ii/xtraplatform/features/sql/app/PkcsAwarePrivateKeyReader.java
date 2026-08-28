/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads a private key from a PEM file, supporting PKCS#1, SEC1, and PKCS#8 formats. If the key is
 * in PKCS#1 or SEC1 format, it will be wrapped in a PKCS#8 structure for use with the JCE.
 */
public final class PkcsAwarePrivateKeyReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PkcsAwarePrivateKeyReader.class);

  public enum PemFormat {
    PKCS8("PRIVATE KEY", null),
    ENCRYPTED_PKCS8("ENCRYPTED PRIVATE KEY", null),
    PKCS1_RSA("RSA PRIVATE KEY", "RSA"),
    SEC1_EC("EC PRIVATE KEY", "EC"),
    OPENSSL_DSA("DSA PRIVATE KEY", "DSA");

    private final String label;
    private final String declaredAlgorithm;

    PemFormat(String label, String declaredAlgorithm) {
      this.label = label;
      this.declaredAlgorithm = declaredAlgorithm;
    }

    public boolean needsWrappingInPkcs8() {
      return declaredAlgorithm != null;
    }

    static PemFormat ofLabel(String label) {
      return Arrays.stream(values())
          .filter(format -> format.label.equals(label))
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "unsupported PEM label \"%s\", expected one of %s"
                          .formatted(
                              label,
                              Arrays.stream(values())
                                  .map(f -> f.label)
                                  .collect(Collectors.joining(", ")))));
    }
  }

  private static final Pattern BEGIN_LINE = Pattern.compile("-----BEGIN (?<label>[A-Z0-9 ]+)-----");

  public static PrivateKey read(Path pemFile) throws IOException {
    String pem = Files.readString(pemFile);
    PemFormat format = detectFormat(pem, pemFile);
    if (format == PemFormat.ENCRYPTED_PKCS8) {
      throw new IllegalArgumentException(
          "%s is an encrypted private key and needs a passphrase".formatted(pemFile));
    }
    if (LOGGER.isDebugEnabled() && format.needsWrappingInPkcs8()) {
      LOGGER.debug("{} is {}, wrapped in PKCS#8 for the JCE", pemFile, format);
    }

    PrivateKey key = new JcaPEMKeyConverter().getPrivateKey(privateKeyInfo(pem, pemFile));

    if (format.declaredAlgorithm != null && !format.declaredAlgorithm.equals(key.getAlgorithm())) {
      throw new IllegalArgumentException(
          "%s declares %s in its PEM header but parsed as %s"
              .formatted(pemFile, format.declaredAlgorithm, key.getAlgorithm()));
    }
    return key;
  }

  public static PemFormat detectFormat(String pem, Path source) {
    String firstLine =
        pem.lines()
            .map(String::strip)
            .filter(line -> !line.isEmpty())
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("%s is empty".formatted(source)));

    Matcher header = BEGIN_LINE.matcher(firstLine);
    if (!header.matches()) {
      throw new IllegalArgumentException(
          "%s does not start with a PEM header: %s".formatted(source, firstLine));
    }
    return PemFormat.ofLabel(header.group("label"));
  }

  private static PrivateKeyInfo privateKeyInfo(String pem, Path source) throws IOException {
    try (PEMParser parser = new PEMParser(new StringReader(pem))) {
      Object parsed = parser.readObject();

      if (parsed instanceof PrivateKeyInfo privateKeyInfo) {
        return privateKeyInfo;
      }
      if (parsed instanceof PEMKeyPair keyPair) {
        return keyPair.getPrivateKeyInfo();
      }
      if (parsed == null) {
        throw new IllegalArgumentException("no PEM object found in %s".formatted(source));
      }
      throw new IllegalArgumentException(
          "%s holds a %s, not a private key".formatted(source, parsed.getClass().getSimpleName()));
    }
  }

  private PkcsAwarePrivateKeyReader() {}
}
