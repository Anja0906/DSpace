/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn.utility;

import static org.dspace.app.ldn.LDNFileTransferException.EXPIRED;
import static org.dspace.app.ldn.LDNFileTransferException.FETCH_FAILED;
import static org.dspace.app.ldn.LDNFileTransferException.TOO_LARGE;
import static org.dspace.app.ldn.LDNFileTransferException.URL_REJECTED;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dspace.app.client.DSpaceHttpClientFactory;
import org.dspace.app.ldn.LDNFileTransferException;
import org.dspace.services.ConfigurationService;

/**
 * Downloads the file offered in a COAR Notify ingest offer into a temporary file. The URL is checked as in
 * {@link org.dspace.content.crosswalk.OREIngestionCrosswalk}, using the ldn.filetransfer.* configuration.
 */
public class RemoteFileFetcher {

    private static final Logger log = LogManager.getLogger(RemoteFileFetcher.class);

    private static final Pattern FILENAME = Pattern.compile("filename\\s*=\\s*\"?([^\";]+)\"?");

    private static final Pattern FILENAME_UTF8 =
        Pattern.compile("filename\\*\\s*=\\s*UTF-8''([^;]+)", Pattern.CASE_INSENSITIVE);

    private final ConfigurationService configurationService;

    public RemoteFileFetcher(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    /**
     * Download the URL into a temporary file.
     *
     * @param url          item URL
     * @param maxSizeBytes maximum file size
     * @return Fetched the downloaded file, to be closed by the caller
     * @throws LDNFileTransferException URL rejected or download failed
     */
    public Fetched fetch(String url, long maxSizeBytes) throws LDNFileTransferException {
        validate(url);

        RequestConfig requestConfig = RequestConfig.custom()
            .setRedirectsEnabled(configurationService.getBooleanProperty("ldn.filetransfer.follow-redirects", false))
            .setConnectTimeout(configurationService.getIntProperty("ldn.filetransfer.connect-timeout-ms", 10000))
            .setSocketTimeout(configurationService.getIntProperty("ldn.filetransfer.read-timeout-ms", 60000))
            .build();

        Path tmp = null;
        try (CloseableHttpClient client = DSpaceHttpClientFactory.getInstance().buildWithRequestConfig(requestConfig);
             CloseableHttpResponse response = client.execute(new HttpGet(url))) {

            int status = response.getStatusLine().getStatusCode();
            if (status == HttpStatus.SC_UNAUTHORIZED || status == HttpStatus.SC_FORBIDDEN
                || status == HttpStatus.SC_GONE) {
                throw new LDNFileTransferException(EXPIRED, "item URL returned HTTP " + status);
            }
            HttpEntity entity = response.getEntity();
            if (status != HttpStatus.SC_OK || entity == null) {
                throw new LDNFileTransferException(FETCH_FAILED, "item URL returned HTTP " + status);
            }
            if (entity.getContentLength() > maxSizeBytes) {
                throw new LDNFileTransferException(TOO_LARGE, "file exceeds " + maxSizeBytes + " bytes");
            }

            tmp = Files.createTempFile("ldn-filetransfer-", ".bin");
            MessageDigest sha256 = ReprDigest.newSha256();
            long size = 0;
            try (InputStream in = entity.getContent(); OutputStream out = Files.newOutputStream(tmp)) {
                byte[] buffer = new byte[8192];
                int n;
                while ((n = in.read(buffer)) != -1) {
                    size += n;
                    if (size > maxSizeBytes) {
                        throw new LDNFileTransferException(TOO_LARGE, "file exceeds " + maxSizeBytes + " bytes");
                    }
                    sha256.update(buffer, 0, n);
                    out.write(buffer, 0, n);
                }
            }

            Fetched fetched = new Fetched(tmp, size, Base64.getEncoder().encodeToString(sha256.digest()),
                headerValue(response, ReprDigest.HEADER),
                StringUtils.trim(StringUtils.substringBefore(headerValue(response, "Content-Type"), ";")),
                filename(headerValue(response, "Content-Disposition"), url));
            tmp = null;
            return fetched;
        } catch (IOException e) {
            throw new LDNFileTransferException(FETCH_FAILED, e.getClass().getSimpleName() + ": " + e.getMessage(), e);
        } finally {
            deleteQuietly(tmp);
        }
    }

    private void validate(String url) throws LDNFileTransferException {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException | NullPointerException e) {
            throw new LDNFileTransferException(URL_REJECTED, "malformed item URL");
        }
        String scheme = StringUtils.lowerCase(uri.getScheme());
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
            throw new LDNFileTransferException(URL_REJECTED, "scheme not allowed: " + scheme);
        }
        String host = StringUtils.lowerCase(uri.getHost());
        if (StringUtils.isBlank(host)) {
            throw new LDNFileTransferException(URL_REJECTED, "item URL has no host");
        }

        if (Arrays.stream(forbiddenHosts()).map(RemoteFileFetcher::hostname).anyMatch(host::equals)) {
            throw new LDNFileTransferException(URL_REJECTED, "forbidden host: " + host);
        }
        if (configurationService.getBooleanProperty("ldn.filetransfer.validateHost", false)
            && Arrays.stream(configurationService.getArrayProperty("ldn.filetransfer.allowedHosts"))
                     .map(RemoteFileFetcher::hostname).noneMatch(host::equals)) {
            throw new LDNFileTransferException(URL_REJECTED, "host not allowed: " + host);
        }
        if (configurationService.getBooleanProperty("ldn.filetransfer.block-local-addresses", true)) {
            try {
                for (InetAddress address : InetAddress.getAllByName(StringUtils.strip(host, "[]"))) {
                    if (address.isLoopbackAddress() || address.isLinkLocalAddress()
                        || address.isAnyLocalAddress() || address.isMulticastAddress()) {
                        throw new LDNFileTransferException(URL_REJECTED, "local address: " + host);
                    }
                }
            } catch (UnknownHostException e) {
                throw new LDNFileTransferException(FETCH_FAILED, "unknown host: " + host);
            }
        }
    }

    private String[] forbiddenHosts() {
        String[] defaults = Stream.of("localhost", "127.0.0.1", "[::1]", "169.254.169.254", "[fd00:ec2::254]",
                configurationService.getProperty("dspace.server.url"),
                configurationService.getProperty("dspace.ui.url"))
            .filter(StringUtils::isNotBlank)
            .toArray(String[]::new);
        return configurationService.getArrayProperty("ldn.filetransfer.forbiddenHostUrls", defaults);
    }

    private static String hostname(String urlOrHost) {
        try {
            String host = new URI(urlOrHost.trim()).getHost();
            if (host != null) {
                return host.toLowerCase(Locale.ROOT);
            }
        } catch (URISyntaxException e) {
            // not a URL, a plain hostname
        }
        return urlOrHost.trim().toLowerCase(Locale.ROOT);
    }

    private static String filename(String contentDisposition, String url) {
        String name = "";
        Matcher utf8Name = FILENAME_UTF8.matcher(StringUtils.defaultString(contentDisposition));
        Matcher plainName = FILENAME.matcher(StringUtils.defaultString(contentDisposition));
        if (utf8Name.find()) {
            name = URLDecoder.decode(utf8Name.group(1).trim(), StandardCharsets.UTF_8);
        } else if (plainName.find()) {
            name = plainName.group(1);
        } else {
            try {
                String path = StringUtils.defaultString(new URI(url).getPath());
                name = URLDecoder.decode(StringUtils.substringAfterLast("/" + path, "/"), StandardCharsets.UTF_8);
            } catch (URISyntaxException e) {
                // no name in the URL
            }
        }
        name = StringUtils.substringAfterLast("/" + name.replace('\\', '/'), "/")
                          .replaceAll("\\p{Cntrl}", "").trim();
        return StringUtils.isBlank(name) ? "file" : name;
    }

    private static String headerValue(CloseableHttpResponse response, String name) {
        Header header = response.getFirstHeader(name);
        return header != null ? header.getValue() : null;
    }

    private static void deleteQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.warn("Could not delete temporary file {}", path, e);
            }
        }
    }

    /**
     * Downloaded file. Closing it deletes the temporary file.
     *
     * @param sha256      base64 SHA-256 of the received bytes
     * @param reprDigest  Repr-Digest header value, null if absent
     * @param contentType media type from Content-Type, null if absent
     */
    public record Fetched(Path path, long size, String sha256, String reprDigest, String contentType,
                          String filename) implements AutoCloseable {

        public InputStream openStream() throws IOException {
            return Files.newInputStream(path);
        }

        @Override
        public void close() {
            deleteQuietly(path);
        }
    }
}
