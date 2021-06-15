package tr.com.example.kafka.contract;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.contract.verifier.messaging.integration.ContractVerifierIntegrationConfiguration;
import org.springframework.cloud.contract.verifier.messaging.kafka.KafkaStubMessagesInitializer;
import org.springframework.cloud.contract.verifier.messaging.noop.NoOpContractVerifierAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;

@Configuration
@ConditionalOnClass({KafkaTemplate.class, EmbeddedKafkaBroker.class})
@ConditionalOnProperty(
        name = {"stubrunner.kafka.enabled"},
        havingValue = "true",
        matchIfMissing = true
)
@AutoConfigureBefore({ContractVerifierIntegrationConfiguration.class, NoOpContractVerifierAutoConfiguration.class})
@ConditionalOnBean({EmbeddedKafkaBroker.class})
public class CustomContractVerifierKafkaConfiguration {
    private static final Log log = LogFactory.getLog(CustomContractVerifierKafkaConfiguration.class);

    public CustomContractVerifierKafkaConfiguration() {
    }

    @Bean
    KafkaStubMessagesInitializer contractVerifierKafkaStubMessagesInitializer() {
        if (log.isDebugEnabled()) {
            log.debug("Registering contract verifier stub messages initializer");
        }

        return new CustomContractVerifierKafkaStubMessagesInitializer();
    }
}
