package com.example.fhir.connect.smt.config;

import com.example.fhir.connect.smt.mapping.MappingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Manages hot-reloading of configuration.
 * Periodically checks S3 for changes and reloads mappings atomically.
 */
public class HotReloadManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(HotReloadManager.class);

    private final S3ConfigLoader configLoader;
    private final AtomicReference<MappingProvider> mappingProviderRef;
    private final Consumer<MappingProvider> onReload;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running;
    private final long intervalSeconds;

    /**
     * Creates a HotReloadManager.
     *
     * @param configLoader           S3 config loader instance
     * @param initialMappingProvider Initial mapping provider
     * @param onReload               Callback when mappings are reloaded
     * @param intervalSeconds        Poll interval in seconds
     */
    public HotReloadManager(S3ConfigLoader configLoader,
            MappingProvider initialMappingProvider,
            Consumer<MappingProvider> onReload,
            long intervalSeconds) {
        this.configLoader = configLoader;
        this.mappingProviderRef = new AtomicReference<>(initialMappingProvider);
        this.onReload = onReload;
        this.intervalSeconds = intervalSeconds;
        this.running = new AtomicBoolean(false);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "smt-hot-reload");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the hot-reload polling.
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(
                    this::checkAndReload,
                    intervalSeconds,
                    intervalSeconds,
                    TimeUnit.SECONDS);
            LOG.info("HotReloadManager started - polling every {} seconds", intervalSeconds);
        }
    }

    /**
     * Stops the hot-reload polling.
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("HotReloadManager stopped");
        }
    }

    /**
     * Checks for config changes and reloads if necessary.
     */
    private void checkAndReload() {
        try {
            String newConfig = configLoader.loadIfChanged();
            if (newConfig != null) {
                LOG.info("Config change detected, reloading mappings...");

                // Create new MappingProvider from updated config
                MappingProvider newProvider = MappingProvider.fromJsonString(newConfig);

                // Atomic swap
                MappingProvider oldProvider = mappingProviderRef.getAndSet(newProvider);

                // Notify callback
                if (onReload != null) {
                    onReload.accept(newProvider);
                }

                LOG.info("Mappings reloaded successfully - {} connector mappings loaded",
                        newProvider.getConnectorNames().size());
            }
        } catch (Exception e) {
            LOG.error("Failed to reload config: {}", e.getMessage(), e);
        }
    }

    /**
     * Gets the current MappingProvider (thread-safe).
     */
    public MappingProvider getCurrentMappingProvider() {
        return mappingProviderRef.get();
    }

    /**
     * Forces an immediate reload (bypasses change detection).
     */
    public void forceReload() {
        try {
            String config = configLoader.loadConfig();
            MappingProvider newProvider = MappingProvider.fromJsonString(config);
            mappingProviderRef.set(newProvider);

            if (onReload != null) {
                onReload.accept(newProvider);
            }

            LOG.info("Forced reload completed - {} connector mappings loaded",
                    newProvider.getConnectorNames().size());
        } catch (Exception e) {
            LOG.error("Forced reload failed: {}", e.getMessage(), e);
            throw new RuntimeException("Forced reload failed", e);
        }
    }

    /**
     * Checks if hot-reload is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() {
        stop();
        if (configLoader != null) {
            configLoader.close();
        }
    }
}
