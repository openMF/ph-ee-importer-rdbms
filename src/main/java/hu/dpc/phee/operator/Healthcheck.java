package hu.dpc.phee.operator;

import io.javalin.Javalin;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class Healthcheck {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void start() {
        int port = 5000;
        logger.info("starting healthcheck service on port {}", port);
        Javalin.create()
                .get("/", ctx -> ctx.result())
                .start(port);
    }

}
