package hu.dpc.phee.operator;


import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;


@Component
public class StsInfo {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void start() {
        try {
            GetCallerIdentityResponse identity = StsClient.builder().build().getCallerIdentity();
            logger.info("running as AWS identity: {}", identity.arn());
        } catch (Exception e) {
            logger.warn("failed to retrieve AWS identity: {}", e.getMessage());
        }
    }
}
