/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn;

/**
 * Failure of a COAR Notify file transfer, with a reason code for the sender.
 */
public class LDNFileTransferException extends Exception {

    public static final String FETCH_FAILED = "fetch-failed";
    public static final String URL_REJECTED = "url-rejected";
    public static final String EXPIRED = "expired";
    public static final String TOO_LARGE = "too-large";
    public static final String DIGEST_MISMATCH = "digest-mismatch";
    public static final String DIGEST_MISSING = "digest-missing";
    public static final String INGEST_FAILED = "ingest-failed";

    private final String reasonCode;

    public LDNFileTransferException(String reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public LDNFileTransferException(String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    public String getReasonCode() {
        return reasonCode;
    }

    /**
     * @return String "reason-code: message"
     */
    public String getSummary() {
        return reasonCode + ": " + getMessage();
    }
}
