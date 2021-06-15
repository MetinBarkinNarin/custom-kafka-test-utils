package tr.com.example.kafka.contract;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.contract.spec.internal.MessagingHeaders;
import org.springframework.cloud.contract.stubrunner.StubFinder;
import org.springframework.cloud.contract.verifier.util.ContentType;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SpringCloudContractTestUtil {
    private static final Log log = LogFactory.getLog(SpringCloudContractTestUtil.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static <T> T getContractBodyData(StubFinder stubFinder, String label, Class<T> dataType) {
        String json = stubFinder.getContracts().entrySet().stream()
                .flatMap(stubConfigurationCollectionEntry -> stubConfigurationCollectionEntry.getValue().stream())
                .filter(contract -> Objects.equals(contract.getLabel(), label))
                .map(contract -> contract.getOutputMessage().getBody().getClientValue().toString())
                .findFirst()
                .orElse("");

        return fromJson(json, dataType);
    }


    public static Map<String, Object> createHeaders(Object key) {
        return new HashMap<String, Object>() {
            {
                put(KafkaHeaders.MESSAGE_KEY, key);
                put(MessagingHeaders.MESSAGING_CONTENT_TYPE, ContentType.JSON.getMimeType());
            }
        };
    }

    public static String createPayloadIncludesHeaders(Object data, Map<String, Object> headers) { // creating payload from Message to include headers
        return createPayload(createMessage(data, headers));
    }

    private static Message<String> createMessage(Object data, Map<String, Object> headers) {
        return MessageBuilder.createMessage(createPayload(data), new MessageHeaders(headers));
    }

    public static String createPayload(Object data) {
        return toJson(data);
    }

    private static String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            log.error("Error in toJson", e);
        }
        return "";
    }

    private static <T> T fromJson(String json, Class<T> c) {
        try {
            return objectMapper.readValue(json, c);
        } catch (JsonProcessingException e) {
            log.error("Error in fromJson", e);
        }
        return null;
    }
}
