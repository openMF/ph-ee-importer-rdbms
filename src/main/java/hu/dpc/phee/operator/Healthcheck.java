package hu.dpc.phee.operator;

import com.sun.net.httpserver.HttpServer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.InetSocketAddress;

@Service
public class Healthcheck {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostConstruct
    public void start() throws IOException {
        int port = 5000;
        logger.info("starting healthcheck service on port {}", port);
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
    }

}
