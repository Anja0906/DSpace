/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn;

import static org.dspace.app.ldn.LDNMessageEntity.QUEUE_STATUS_FAILED;
import static org.dspace.app.ldn.LDNMessageEntity.QUEUE_STATUS_PROCESSED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import org.dspace.AbstractIntegrationTestWithDatabase;
import org.dspace.app.ldn.factory.NotifyServiceFactory;
import org.dspace.app.ldn.model.Notification;
import org.dspace.app.ldn.model.Service;
import org.dspace.app.ldn.model.Url;
import org.dspace.app.ldn.processor.LDNProcessor;
import org.dspace.app.ldn.service.LDNMessageService;
import org.dspace.app.ldn.utility.ReprDigest;
import org.dspace.builder.CollectionBuilder;
import org.dspace.builder.CommunityBuilder;
import org.dspace.builder.NotifyServiceBuilder;
import org.dspace.content.Bitstream;
import org.dspace.content.Collection;
import org.dspace.content.Item;
import org.dspace.content.MetadataValue;
import org.dspace.content.factory.ContentServiceFactory;
import org.dspace.content.service.BitstreamService;
import org.dspace.content.service.ItemService;
import org.dspace.content.service.WorkspaceItemService;
import org.dspace.core.Constants;
import org.dspace.services.ConfigurationService;
import org.dspace.services.factory.DSpaceServicesFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for {@link org.dspace.app.ldn.processor.LDNFileTransferProcessor}, with a local HTTP
 * server in place of TeslaRIS.
 */
public class LDNFileTransferProcessorIT extends AbstractIntegrationTestWithDatabase {

    private static final String ORIGIN_ID = "https://teslaris.example.org";
    private static final String ORIGIN_INBOX = "https://teslaris.example.org/api/coar-notify/inbox";
    private static final String SOURCE_IP = "127.0.0.1";
    private static final byte[] PDF = "%PDF-1.4\nthesis test file\n%%EOF\n".getBytes(StandardCharsets.US_ASCII);

    private final ConfigurationService configurationService =
        DSpaceServicesFactory.getInstance().getConfigurationService();
    private final LDNMessageService ldnMessageService = NotifyServiceFactory.getInstance().getLDNMessageService();
    private final ItemService itemService = ContentServiceFactory.getInstance().getItemService();
    private final BitstreamService bitstreamService = ContentServiceFactory.getInstance().getBitstreamService();
    private final WorkspaceItemService workspaceItemService =
        ContentServiceFactory.getInstance().getWorkspaceItemService();

    private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();
    private final List<String> notificationIds = new ArrayList<>();
    private HttpServer server;
    private String baseUrl;
    private Collection collection;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        startFileServer();

        context.turnOffAuthorisationSystem();
        parentCommunity = CommunityBuilder.createCommunity(context).withName("COAR Notify Transfers").build();
        collection = CollectionBuilder.createCollection(context, parentCommunity)
                                      .withName("Incoming from TeslaRIS").build();
        NotifyServiceBuilder.createNotifyServiceBuilder(context, "TeslaRIS")
                            .withUrl(ORIGIN_ID)
                            .withLdnUrl(ORIGIN_INBOX)
                            .withLowerIp(SOURCE_IP)
                            .withUpperIp(SOURCE_IP)
                            .withStatus(true)
                            .build();
        context.restoreAuthSystemState();

        configurationService.setProperty("ldn.filetransfer.enabled", true);
        configurationService.setProperty("ldn.filetransfer.collection", collection.getHandle());
        configurationService.setProperty("ldn.filetransfer.eperson", admin.getEmail());
        configurationService.setProperty("ldn.filetransfer.forbiddenHostUrls", "169.254.169.254");
        configurationService.setProperty("ldn.filetransfer.block-local-addresses", false);
        configurationService.setProperty("ldn.filetransfer.max-size-bytes", 1_000_000L);
        configurationService.setProperty("ldn.filetransfer.require-digest", false);
    }

    @After
    @Override
    public void destroy() throws Exception {
        if (server != null) {
            server.stop(0);
        }
        context.turnOffAuthorisationSystem();
        for (String id : notificationIds) {
            for (Item item : itemsFor(id)) {
                itemService.delete(context, item);
            }
            LDNMessageEntity msg = ldnMessageService.find(context, id);
            if (msg != null) {
                ldnMessageService.delete(context, msg);
            }
        }
        context.restoreAuthSystemState();
        for (String key : new String[] {"enabled", "collection", "eperson", "forbiddenHostUrls",
            "block-local-addresses", "max-size-bytes", "require-digest"}) {
            configurationService.setProperty("ldn.filetransfer." + key, null);
        }
        super.destroy();
    }

    @Test
    public void offerWithValidDigestCreatesArchivedItemWithFile() throws Exception {
        String id = queueOffer(baseUrl + "/files/ok?transferToken=abc&expires=9999999999", "application/pdf");

        ldnMessageService.extractAndProcessMessageFromQueue(context);

        assertEquals(QUEUE_STATUS_PROCESSED, ldnMessageService.find(context, id).getQueueStatus());
        List<Item> items = itemsFor(id);
        assertEquals(1, items.size());
        Item item = items.get(0);
        assertTrue(item.isArchived());
        assertEquals(collection.getID(), item.getOwningCollection().getID());
        assertEquals("Title of the publication",
            itemService.getMetadataFirstValue(item, "dc", "title", null, Item.ANY));
        assertEquals(ORIGIN_ID + "/documents/123",
            itemService.getMetadataFirstValue(item, "dc", "source", null, Item.ANY));
        assertThat(values(item, "dc", "identifier", "uri"), hasItem("https://doi.org/10.5555/12345680"));

        List<Bitstream> bitstreams = item.getBundles(Constants.CONTENT_BUNDLE_NAME).get(0).getBitstreams();
        assertEquals(1, bitstreams.size());
        Bitstream bitstream = bitstreams.get(0);
        assertEquals("paper.pdf", bitstream.getName());
        assertEquals("application/pdf", bitstream.getFormat(context).getMIMEType());
        try (InputStream in = bitstreamService.retrieve(context, bitstream)) {
            assertArrayEquals(PDF, in.readAllBytes());
        }
        assertEquals(1, hits("/files/ok"));
    }

    @Test
    public void offerWithoutDigestIsAcceptedUnlessRequired() throws Exception {
        String accepted = queueOffer(baseUrl + "/files/no-digest", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertEquals(QUEUE_STATUS_PROCESSED, ldnMessageService.find(context, accepted).getQueueStatus());
        assertEquals(1, itemsFor(accepted).size());

        configurationService.setProperty("ldn.filetransfer.require-digest", true);
        String rejected = queueOffer(baseUrl + "/files/no-digest", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertFailedWithoutItem(rejected);
    }

    @Test
    public void digestMismatchFailsAndLeavesNothingBehind() throws Exception {
        String id = queueOffer(baseUrl + "/files/bad-digest", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertFailedWithoutItem(id);
    }

    @Test
    public void expiredTokenFails() throws Exception {
        String id = queueOffer(baseUrl + "/files/expired", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertFailedWithoutItem(id);
    }

    @Test
    public void oversizeFileFails() throws Exception {
        configurationService.setProperty("ldn.filetransfer.max-size-bytes", 10L);
        String id = queueOffer(baseUrl + "/files/ok", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertFailedWithoutItem(id);
    }

    @Test
    public void forbiddenHostsAndSchemesAreNeverFetched() throws Exception {
        String metadata = queueOffer("http://169.254.169.254/latest/meta-data/", "text/plain");
        String file = queueOffer("file:///etc/passwd", "text/plain");
        configurationService.setProperty("ldn.filetransfer.forbiddenHostUrls", "127.0.0.1");
        String loopback = queueOffer(baseUrl + "/files/ok", "application/pdf");

        ldnMessageService.extractAndProcessMessageFromQueue(context);

        assertFailedWithoutItem(metadata);
        assertFailedWithoutItem(file);
        assertFailedWithoutItem(loopback);
        assertEquals("the forbidden local URL must not be requested", 0, hits("/files/ok"));
    }

    @Test
    public void localAddressesAreBlockedByResolution() throws Exception {
        // "localhost" is not on the forbidden list here, but resolves to a loopback address
        configurationService.setProperty("ldn.filetransfer.block-local-addresses", true);
        String id = queueOffer(baseUrl.replace("127.0.0.1", "localhost") + "/files/ok", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        assertFailedWithoutItem(id);
        assertEquals(0, hits("/files/ok"));
    }

    @Test
    public void repeatedOfferDoesNotCreateSecondItem() throws Exception {
        Notification offer = offer(baseUrl + "/files/ok", "application/pdf");
        LDNProcessor processor = DSpaceServicesFactory.getInstance().getServiceManager()
                                                      .getServiceByName("offerIngestAction", LDNProcessor.class);
        assertNotNull(processor);

        processor.process(context, offer);
        processor.process(context, offer);

        assertEquals(1, itemsFor(offer.getId()).size());
        assertEquals("the file is fetched only once", 1, hits("/files/ok"));
    }

    @Test
    public void disabledFallsBackToOriginalProcessor() throws Exception {
        configurationService.setProperty("ldn.filetransfer.enabled", false);
        String id = queueOffer(baseUrl + "/files/ok", "application/pdf");
        ldnMessageService.extractAndProcessMessageFromQueue(context);
        // the original processor only handles offers about existing local items
        assertFailedWithoutItem(id);
        assertEquals(0, hits("/files/ok"));
    }

    @Test
    public void reprDigestParsing() {
        assertEquals("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
            ReprDigest.parseSha256("sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:"));
        assertEquals("47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
            ReprDigest.parseSha256("sha-512=:AAAA:, SHA-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:"));
        assertEquals(null, ReprDigest.parseSha256("sha-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="));
        assertEquals(null, ReprDigest.parseSha256("sha-256=:AAAA:"));
        assertEquals(null, ReprDigest.parseSha256(null));
    }

    private void assertFailedWithoutItem(String id) throws Exception {
        LDNMessageEntity msg = ldnMessageService.find(context, id);
        assertNotNull(msg);
        assertEquals("queue status of " + id, QUEUE_STATUS_FAILED, msg.getQueueStatus());
        assertTrue("no archived item for " + id, itemsFor(id).isEmpty());
        assertTrue("no leftover workspace item", workspaceItemService.findByCollection(context, collection).isEmpty());
    }

    private String queueOffer(String itemUrl, String mediaType) throws Exception {
        Notification notification = offer(itemUrl, mediaType);
        context.turnOffAuthorisationSystem();
        LDNMessageEntity msg = ldnMessageService.create(context, notification, SOURCE_IP);
        // H2 stores queue_timeout without time zone, so on a non-UTC machine it can be in the future
        msg.setQueueTimeout(Instant.now().minus(1, ChronoUnit.DAYS));
        ldnMessageService.update(context, msg);
        context.restoreAuthSystemState();
        assertEquals(LDNMessageEntity.QUEUE_STATUS_QUEUED, msg.getQueueStatus());
        return notification.getId();
    }

    private Notification offer(String itemUrl, String mediaType) {
        Notification n = new Notification();
        String id = "urn:uuid:" + UUID.randomUUID();
        notificationIds.add(id);
        n.setId(id);
        n.setType(Set.of("Offer", "coar-notify:IngestAction"));
        Service origin = new Service();
        origin.setId(ORIGIN_ID);
        origin.setInbox(ORIGIN_INBOX);
        origin.setType(Set.of("Service"));
        n.setOrigin(origin);
        Service target = new Service();
        target.setId(configurationService.getProperty("dspace.ui.url"));
        target.setInbox(configurationService.getProperty("ldn.notify.inbox"));
        target.setType(Set.of("Service"));
        n.setTarget(target);
        org.dspace.app.ldn.model.Actor actor = new org.dspace.app.ldn.model.Actor();
        actor.setId(ORIGIN_ID);
        actor.setName("TeslaRIS");
        actor.setType(Set.of("Service"));
        n.setActor(actor);
        org.dspace.app.ldn.model.Object object = new org.dspace.app.ldn.model.Object();
        object.setId(ORIGIN_ID + "/documents/123");
        object.setIetfCiteAs("https://doi.org/10.5555/12345680");
        object.setTitle("Title of the publication");
        object.setType(Set.of("Page", "sorg:AboutPage"));
        Url url = new Url();
        url.setId(itemUrl);
        url.setMediaType(mediaType);
        url.setType(Set.of("Article", "sorg:ScholarlyArticle"));
        object.setUrl(url);
        n.setObject(object);
        return n;
    }

    private List<Item> itemsFor(String notificationId) throws Exception {
        List<Item> result = new ArrayList<>();
        Iterator<Item> it = itemService.findArchivedByMetadataField(context, "dc", "identifier", "other",
            notificationId);
        it.forEachRemaining(result::add);
        return result;
    }

    private List<String> values(Item item, String schema, String element, String qualifier) {
        List<String> result = new ArrayList<>();
        for (MetadataValue value : itemService.getMetadata(item, schema, element, qualifier, Item.ANY)) {
            result.add(value.getValue());
        }
        return result;
    }

    private int hits(String path) {
        AtomicInteger count = hits.get(path);
        return count == null ? 0 : count.get();
    }

    private void startFileServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        String sha256 = Base64.getEncoder().encodeToString(ReprDigest.newSha256().digest(PDF));
        serve("/files/ok", 200, "sha-256=:" + sha256 + ":");
        serve("/files/no-digest", 200, null);
        serve("/files/bad-digest", 200, "sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:");
        serve("/files/expired", 410, null);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private void serve(String path, int status, String reprDigest) {
        server.createContext(path, exchange -> {
            hits.computeIfAbsent(path, p -> new AtomicInteger()).incrementAndGet();
            if (status != 200) {
                exchange.sendResponseHeaders(status, -1);
                exchange.close();
                return;
            }
            exchange.getResponseHeaders().add("Content-Type", "application/pdf");
            exchange.getResponseHeaders().add("Content-Disposition", "attachment; filename=\"paper.pdf\"");
            if (reprDigest != null) {
                exchange.getResponseHeaders().add(ReprDigest.HEADER, reprDigest);
            }
            exchange.sendResponseHeaders(200, PDF.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(PDF);
            }
        });
    }
}
