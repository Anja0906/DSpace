/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn.utility;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.apache.commons.lang3.StringUtils;

/**
 * RFC 9530 Repr-Digest header, e.g. {@code sha-256=:X48E9q...=:}
 */
public final class ReprDigest {

    public static final String HEADER = "Repr-Digest";

    private ReprDigest() {
    }

    /**
     * @param headerValue Repr-Digest header value
     * @return String base64 sha-256 digest, or null if there is no valid sha-256 member
     */
    public static String parseSha256(String headerValue) {
        if (headerValue == null) {
            return null;
        }
        for (String member : headerValue.split(",")) {
            String key = StringUtils.substringBefore(member, "=").trim();
            String value = StringUtils.substringBefore(StringUtils.substringAfter(member, "="), ";").trim();
            if (key.equalsIgnoreCase("sha-256") && value.length() > 2
                && value.startsWith(":") && value.endsWith(":")) {
                String digest = value.substring(1, value.length() - 1);
                try {
                    return Base64.getDecoder().decode(digest).length == 32 ? digest : null;
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        }
        return null;
    }

    public static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
