/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 * http://www.dspace.org/license/
 */
package org.dspace.app.ldn.action;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dspace.app.client.DSpaceHttpClientFactory;
import org.dspace.app.ldn.LDNMessageEntity;
import org.dspace.app.ldn.NotifyServiceEntity;
import org.dspace.app.ldn.model.Notification;
import org.dspace.app.ldn.service.LDNMessageService;
import org.dspace.app.ldn.service.NotifyService;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.handle.service.HandleService;
import org.dspace.services.ConfigurationService;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Replies to an Offer + coar-notify:IngestAction with an Accept (the created item as context) or a Reject
 * (summary "reason-code: text"). The reply goes to the inbox of the registered notify service that sent the
 * offer, and is stored as an outgoing LDN message: PROCESSED if delivered, FAILED if not.
 */
public class SendAcknowledgementAction implements LDNAction {

    private static final Logger log = LogManager.getLogger(SendAcknowledgementAction.class);

    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private ConfigurationService configurationService;

    @Autowired
    private LDNMessageService ldnMessageService;

    @Autowired
    private NotifyService notifyService;

    @Autowired
    private HandleService handleService;

    @Override
    public LDNActionStatus execute(Context context, Notification offer, Item item) throws Exception {
        accept(context, offer, item);
        return LDNActionStatus.CONTINUE;
    }

    /**
     * @return boolean true if the sender's inbox accepted the message
     */
    public boolean accept(Context context, Notification offer, Item item) {
        return send(context, offer, "Accept", item, null);
    }

    /**
     * @param summary "reason-code: text"
     * @return boolean true if the sender's inbox accepted the message
     */
    public boolean reject(Context context, Notification offer, String summary) {
        return send(context, offer, "Reject", null, summary);
    }

    private boolean send(Context context, Notification offer, String type, Item item, String summary) {
        try {
            NotifyServiceEntity sender = offer.getOrigin() != null
                ? notifyService.findByLdnUrl(context, offer.getOrigin().getInbox()) : null;
            if (sender == null) {
                log.warn("Not sending {} for offer {}: origin.inbox is not a registered notify service",
                    type, offer.getId());
                return false;
            }

            ObjectNode ack = build(context, offer, type, item, summary, sender);
            String json = mapper.writeValueAsString(ack);
            boolean delivered = post(sender.getLdnUrl(), json);
            record(context, ack.get("id").asText(), json, type, offer, item, sender, delivered);
            if (delivered) {
                log.info("{} for offer {} delivered to {}", type, offer.getId(), sender.getLdnUrl());
            } else {
                log.warn("{} for offer {} could NOT be delivered to {}", type, offer.getId(), sender.getLdnUrl());
            }
            return delivered;
        } catch (Exception e) {
            log.error("Failed to send {} for offer {}", type, offer.getId(), e);
            return false;
        }
    }

    private ObjectNode build(Context context, Notification offer, String type, Item item, String summary,
                             NotifyServiceEntity sender) throws Exception {
        String uiUrl = configurationService.getProperty("dspace.ui.url");
        String senderId = offer.getOrigin().getId() != null ? offer.getOrigin().getId() : sender.getUrl();

        ObjectNode ack = mapper.createObjectNode();
        ack.set("@context", mapper.createArrayNode()
                                  .add("https://www.w3.org/ns/activitystreams")
                                  .add("https://coar-notify.net"));
        ack.put("id", "urn:uuid:" + UUID.randomUUID());
        ack.put("type", type);
        ack.put("inReplyTo", offer.getId());
        ack.set("actor", service(uiUrl, configurationService.getProperty("dspace.name"), null));
        ack.set("origin", service(uiUrl, null, configurationService.getProperty("ldn.notify.inbox")));
        ack.set("target", service(senderId, null, sender.getLdnUrl()));
        ack.set("object", originalOffer(context, offer));
        if (item != null) {
            ack.set("context", itemContext(context, item, uiUrl));
        }
        if (summary != null) {
            ack.put("summary", summary);
        }
        return ack;
    }

    private ObjectNode originalOffer(Context context, Notification offer) throws Exception {
        LDNMessageEntity stored = ldnMessageService.find(context, offer.getId());
        ObjectNode object = (ObjectNode) (stored != null && stored.getMessage() != null
            ? mapper.readTree(stored.getMessage())
            : mapper.valueToTree(offer));
        object.remove("@context");
        removeNulls(object);
        return object;
    }

    private ObjectNode itemContext(Context context, Item item, String uiUrl) throws SQLException {
        ObjectNode itemContext = mapper.createObjectNode();
        itemContext.put("id", uiUrl + "/items/" + item.getID());
        if (item.getHandle() != null) {
            itemContext.put("ietf:cite-as", handleService.getCanonicalForm(item.getHandle()));
        }
        Bitstream file = primaryFile(item);
        if (file != null) {
            ObjectNode itemUrl = mapper.createObjectNode();
            itemUrl.put("id", uiUrl + "/bitstreams/" + file.getID() + "/download");
            if (file.getFormat(context) != null) {
                itemUrl.put("mediaType", file.getFormat(context).getMIMEType());
            }
            itemUrl.set("type", mapper.createArrayNode().add("Article").add("sorg:ScholarlyArticle"));
            itemContext.set("ietf:item", itemUrl);
        }
        itemContext.set("type", mapper.createArrayNode().add("Page").add("sorg:AboutPage"));
        return itemContext;
    }

    private Bitstream primaryFile(Item item) {
        List<Bundle> bundles = item.getBundles(Constants.CONTENT_BUNDLE_NAME);
        if (bundles.isEmpty()) {
            return null;
        }
        Bundle bundle = bundles.get(0);
        if (bundle.getPrimaryBitstream() != null) {
            return bundle.getPrimaryBitstream();
        }
        return bundle.getBitstreams().isEmpty() ? null : bundle.getBitstreams().get(0);
    }

    private ObjectNode service(String id, String name, String inbox) {
        ObjectNode node = mapper.createObjectNode();
        node.put("id", id);
        if (name != null) {
            node.put("name", name);
        }
        if (inbox != null) {
            node.put("inbox", inbox);
        }
        node.put("type", "Service");
        return node;
    }

    private boolean post(String inbox, String json) {
        RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(configurationService.getIntProperty("ldn.filetransfer.connect-timeout-ms", 10000))
            .setSocketTimeout(configurationService.getIntProperty("ldn.filetransfer.read-timeout-ms", 60000))
            .build();
        HttpPost post = new HttpPost(inbox);
        post.setEntity(new StringEntity(json, ContentType.create("application/ld+json", "UTF-8")));
        try (CloseableHttpClient client = DSpaceHttpClientFactory.getInstance().buildWithRequestConfig(requestConfig);
             CloseableHttpResponse response = client.execute(post)) {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 200 && status <= 202) {
                return true;
            }
            log.warn("Inbox {} answered HTTP {}", inbox, status);
        } catch (Exception e) {
            log.warn("Inbox {} unreachable: {}", inbox, e.toString());
        }
        return false;
    }

    private void record(Context context, String ackId, String json, String type, Notification offer, Item item,
                        NotifyServiceEntity sender, boolean delivered) {
        try {
            LDNMessageEntity message = ldnMessageService.create(context, ackId);
            message.setMessage(json);
            message.setType(type);
            message.setActivityStreamType(type);
            message.setCoarNotifyType("coar-notify:IngestAction");
            message.setTarget(sender);
            message.setObject(item);
            message.setInReplyTo(ldnMessageService.find(context, offer.getId()));
            message.setQueueStatus(delivered ? LDNMessageEntity.QUEUE_STATUS_PROCESSED
                                             : LDNMessageEntity.QUEUE_STATUS_FAILED);
            message.setQueueAttempts(1);
            message.setQueueLastStartTime(Instant.now());
            message.setQueueTimeout(Instant.now());
            ldnMessageService.update(context, message);
        } catch (Exception e) {
            log.error("Could not record {} {} for offer {}", type, ackId, offer.getId(), e);
        }
    }

    private static void removeNulls(JsonNode node) {
        if (node instanceof ObjectNode object) {
            Iterator<Map.Entry<String, JsonNode>> fields = object.fields();
            while (fields.hasNext()) {
                JsonNode value = fields.next().getValue();
                if (value == null || value.isNull()) {
                    fields.remove();
                } else {
                    removeNulls(value);
                }
            }
        } else if (node instanceof ArrayNode array) {
            array.forEach(SendAcknowledgementAction::removeNulls);
        }
    }
}
