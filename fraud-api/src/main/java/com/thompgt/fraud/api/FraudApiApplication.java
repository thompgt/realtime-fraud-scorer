package com.thompgt.fraud.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Query and rule-admin API. Endpoints land in Phase 5. */
@SpringBootApplication
public class FraudApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(FraudApiApplication.class, args);
    }
}
