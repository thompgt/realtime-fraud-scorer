package com.thompgt.fraud.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for the synthetic transaction generator. Implemented in Phase 2. */
public final class GeneratorMain {

    private static final Logger LOG = LoggerFactory.getLogger(GeneratorMain.class);

    private GeneratorMain() {
    }

    public static void main(String[] args) {
        LOG.info("fraud-generator scaffold: no producer wired up yet (Phase 2).");
    }
}
