/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn.processor;

import static org.dspace.app.ldn.LDNFileTransferException.DIGEST_MISMATCH;
import static org.dspace.app.ldn.LDNFileTransferException.DIGEST_MISSING;
import static org.dspace.app.ldn.LDNFileTransferException.INGEST_FAILED;
import static org.dspace.app.ldn.LDNFileTransferException.URL_REJECTED;

import java.io.InputStream;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dspace.app.ldn.LDNFileTransferException;
import org.dspace.app.ldn.action.LDNAction;
import org.dspace.app.ldn.action.LDNActionStatus;
import org.dspace.app.ldn.action.SendAcknowledgementAction;
import org.dspace.app.ldn.model.Notification;
import org.dspace.app.ldn.model.Url;
import org.dspace.app.ldn.utility.RemoteFileFetcher;
import org.dspace.app.ldn.utility.ReprDigest;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.BitstreamFormat;
import org.dspace.content.Bundle;
import org.dspace.content.Collection;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.WorkspaceItem;
import org.dspace.content.service.BitstreamFormatService;
import org.dspace.content.service.BitstreamService;
import org.dspace.content.service.BundleService;
import org.dspace.content.service.CollectionService;
import org.dspace.content.service.InstallItemService;
import org.dspace.content.service.ItemService;
import org.dspace.content.service.WorkspaceItemService;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.eperson.EPerson;
import org.dspace.eperson.service.EPersonService;
import org.dspace.handle.service.HandleService;
import org.dspace.services.ConfigurationService;
import org.dspace.util.UUIDUtils;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Linked Data Notification processor for Offer + coar-notify:IngestAction. The file referenced by
 * object.ietf:item.id is downloaded, checked against its Repr-Digest and archived as a new item in the
 * collection configured by ldn.filetransfer.collection. The sender gets an Accept or a Reject.
 */
public class LDNFileTransferProcessor implements LDNProcessor {

    private static final Logger log = LogManager.getLogger(LDNFileTransferProcessor.class);

    @Autowired
    private ConfigurationService configurationService;

    @Autowired
    private EPersonService ePersonService;

    @Autowired
    private CollectionService collectionService;

    @Autowired
    private HandleService handleService;

    @Autowired
    private WorkspaceItemService workspaceItemService;

    @Autowired
    private ItemService itemService;

    @Autowired
    private BundleService bundleService;

    @Autowired
    private BitstreamService bitstreamService;

    @Autowired
    private BitstreamFormatService bitstreamFormatService;

    @Autowired
    private InstallItemService installItemService;

    private List<LDNAction> actions = new ArrayList<>();

    private LDNProcessor fallbackProcessor;

    private SendAcknowledgementAction acknowledgementAction;

    /**
     * Ingest the offered file, reply Accept and run the actions on the new item, or reply Reject. A repeated
     * offer is answered with the item created the first time.
     *
     * @param context      current context
     * @param notification received offer
     * @throws Exception the file could not be transferred
     */
    @Override
    public void process(Context context, Notification notification) throws Exception {
        if (!configurationService.getBooleanProperty("ldn.filetransfer.enabled", false)) {
            fallbackProcessor.process(context, notification);
            return;
        }
        EPerson previousUser = context.getCurrentUser();
        context.turnOffAuthorisationSystem();
        try {
            Item item;
            try {
                item = findOrIngest(context, notification);
            } catch (Exception e) {
                LDNFileTransferException failure = e instanceof LDNFileTransferException transferException
                    ? transferException
                    : new LDNFileTransferException(INGEST_FAILED, e.getClass().getSimpleName() + ": "
                        + e.getMessage(), e);
                log.warn("File transfer for offer {} failed: {}", notification.getId(), failure.getSummary());
                if (acknowledgementAction != null) {
                    acknowledgementAction.reject(context, notification, failure.getSummary());
                }
                throw failure;
            }
            // the item exists, so a failing action must not turn the Accept into a Reject
            try {
                if (acknowledgementAction != null) {
                    acknowledgementAction.accept(context, notification, item);
                }
                runActions(context, notification, item);
            } catch (Exception e) {
                log.error("Action failed for offer {}, item {} was created", notification.getId(), item.getID(), e);
            }
        } finally {
            context.restoreAuthSystemState();
            context.setCurrentUser(previousUser);
        }
    }

    private Item findOrIngest(Context context, Notification notification) throws Exception {
        Url itemUrl = notification.getObject() != null ? notification.getObject().getUrl() : null;
        if (itemUrl == null || StringUtils.isBlank(itemUrl.getId())) {
            throw new LDNFileTransferException(URL_REJECTED, "offer has no object.ietf:item.id");
        }
        Item existing = findExisting(context, notification.getId());
        if (existing != null) {
            log.info("Offer {} was already ingested as item {}", notification.getId(), existing.getID());
            context.setCurrentUser(existing.getSubmitter());
            return existing;
        }
        Collection collection = resolveCollection(context);
        context.setCurrentUser(resolveSubmitter(context));
        return ingest(context, notification, itemUrl, collection);
    }

    private Item ingest(Context context, Notification notification, Url itemUrl, Collection collection)
        throws LDNFileTransferException {
        long maxSize = configurationService.getLongProperty("ldn.filetransfer.max-size-bytes", 524_288_000L);
        try (RemoteFileFetcher.Fetched fetched =
                 new RemoteFileFetcher(configurationService).fetch(itemUrl.getId(), maxSize)) {
            verifyDigest(fetched);
            Item item = createItem(context, notification, itemUrl, collection, fetched);
            log.info("Ingested offer {} as item {} ({} bytes)", notification.getId(), item.getID(), fetched.size());
            return item;
        }
    }

    private void verifyDigest(RemoteFileFetcher.Fetched fetched) throws LDNFileTransferException {
        if (fetched.reprDigest() == null) {
            if (configurationService.getBooleanProperty("ldn.filetransfer.require-digest", false)) {
                throw new LDNFileTransferException(DIGEST_MISSING, "no Repr-Digest header on the item URL");
            }
            log.warn("No Repr-Digest header, file integrity not verified");
            return;
        }
        String expected = ReprDigest.parseSha256(fetched.reprDigest());
        if (!fetched.sha256().equals(expected)) {
            throw new LDNFileTransferException(DIGEST_MISMATCH,
                "expected sha-256 " + expected + ", received " + fetched.sha256());
        }
    }

    private Item createItem(Context context, Notification notification, Url itemUrl, Collection collection,
                            RemoteFileFetcher.Fetched fetched) throws LDNFileTransferException {
        WorkspaceItem workspaceItem = null;
        try {
            workspaceItem = workspaceItemService.create(context, collection, false);
            Item item = workspaceItem.getItem();
            addMetadata(context, item, notification, fetched.filename());

            Bundle bundle = bundleService.create(context, item, Constants.CONTENT_BUNDLE_NAME);
            Bitstream bitstream;
            try (InputStream in = fetched.openStream()) {
                bitstream = bitstreamService.create(context, bundle, in);
            }
            bitstream.setName(context, fetched.filename());
            bitstreamService.setFormat(context, bitstream,
                resolveFormat(context, bitstream, itemUrl.getMediaType(), fetched.contentType()));
            bitstreamService.update(context, bitstream);
            bundle.setPrimaryBitstreamID(bitstream);
            bundleService.update(context, bundle);

            Item installed = installItemService.installItem(context, workspaceItem);
            workspaceItem = null;
            return installed;
        } catch (Exception e) {
            throw new LDNFileTransferException(INGEST_FAILED, e.getClass().getSimpleName() + ": " + e.getMessage(), e);
        } finally {
            if (workspaceItem != null) {
                try {
                    workspaceItemService.deleteAll(context, workspaceItem);
                } catch (Exception e) {
                    log.error("Could not delete workspace item {}", workspaceItem.getID(), e);
                }
            }
        }
    }

    private void addMetadata(Context context, Item item, Notification notification, String filename)
        throws SQLException {
        String title = notification.getObject().getTitle();
        itemService.addMetadata(context, item, "dc", "title", null, null,
            StringUtils.isNotBlank(title) ? title : filename);
        if (StringUtils.isNotBlank(notification.getObject().getIetfCiteAs())) {
            itemService.addMetadata(context, item, "dc", "identifier", "uri", null,
                notification.getObject().getIetfCiteAs());
        }
        if (StringUtils.isNotBlank(notification.getObject().getId())) {
            itemService.addMetadata(context, item, "dc", "source", null, null, notification.getObject().getId());
        }
        itemService.addMetadata(context, item, "dc", "identifier", "other", null, notification.getId());
        String origin = notification.getOrigin() != null ? notification.getOrigin().getId() : null;
        itemService.addMetadata(context, item, "dc", "description", "provenance", "en",
            "Ingested via COAR Notify from " + origin + ", notification " + notification.getId()
                + " on " + Instant.now());
    }

    private BitstreamFormat resolveFormat(Context context, Bitstream bitstream, String offeredMediaType,
                                          String responseMediaType) throws SQLException {
        for (String mediaType : new String[] {offeredMediaType, responseMediaType}) {
            if (StringUtils.isNotBlank(mediaType)) {
                BitstreamFormat format = bitstreamFormatService.findByMIMEType(context, mediaType);
                if (format != null) {
                    return format;
                }
            }
        }
        return bitstreamFormatService.guessFormat(context, bitstream);
    }

    private Item findExisting(Context context, String notificationId) throws SQLException, AuthorizeException {
        Iterator<Item> items = itemService.findArchivedByMetadataField(context, "dc", "identifier", "other",
            notificationId);
        return items.hasNext() ? items.next() : null;
    }

    private EPerson resolveSubmitter(Context context) throws SQLException, LDNFileTransferException {
        String email = configurationService.getProperty("ldn.filetransfer.eperson",
            configurationService.getProperty("mail.admin"));
        EPerson ePerson = StringUtils.isNotBlank(email) ? ePersonService.findByEmail(context, email) : null;
        if (ePerson == null) {
            throw new LDNFileTransferException(INGEST_FAILED, "ldn.filetransfer.eperson not found: " + email);
        }
        return ePerson;
    }

    private Collection resolveCollection(Context context) throws SQLException, LDNFileTransferException {
        String ref = StringUtils.trimToEmpty(configurationService.getProperty("ldn.filetransfer.collection"));
        if (ref.isEmpty()) {
            throw new LDNFileTransferException(INGEST_FAILED, "ldn.filetransfer.collection is not set");
        }
        UUID uuid = UUIDUtils.fromString(ref);
        DSpaceObject dso = uuid != null
            ? collectionService.find(context, uuid)
            : handleService.resolveToObject(context, ref);
        if (!(dso instanceof Collection)) {
            throw new LDNFileTransferException(INGEST_FAILED, "ldn.filetransfer.collection is not a collection: "
                + ref);
        }
        return (Collection) dso;
    }

    private void runActions(Context context, Notification notification, Item item) throws Exception {
        for (LDNAction action : actions) {
            log.info("Running action {} for notification {}", action.getClass().getSimpleName(), notification.getId());
            if (action.execute(context, notification, item) == LDNActionStatus.ABORT) {
                break;
            }
        }
    }

    public List<LDNAction> getActions() {
        return actions;
    }

    public void setActions(List<LDNAction> actions) {
        this.actions = actions;
    }

    public LDNProcessor getFallbackProcessor() {
        return fallbackProcessor;
    }

    public void setFallbackProcessor(LDNProcessor fallbackProcessor) {
        this.fallbackProcessor = fallbackProcessor;
    }

    public SendAcknowledgementAction getAcknowledgementAction() {
        return acknowledgementAction;
    }

    public void setAcknowledgementAction(SendAcknowledgementAction acknowledgementAction) {
        this.acknowledgementAction = acknowledgementAction;
    }
}
