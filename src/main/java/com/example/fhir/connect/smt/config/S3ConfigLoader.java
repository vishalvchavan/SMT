package com.example.fhir.connect.smt.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Loads configuration files from S3/MinIO.
 * Supports change detection via ETag/hash comparison.
 */
public class S3ConfigLoader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(S3ConfigLoader.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private String lastETag;
    private String lastContentHash;
    private final int maxAttempts;
    private final long initialBackoffMs;

    /**
     * Creates an S3ConfigLoader for the given bucket and key.
     *
     * @param endpoint  S3/MinIO endpoint URL (e.g., http://minio:9000)
     * @param bucket    Bucket name
     * @param key       Object key (path to config file)
     * @param accessKey AWS/MinIO access key
     * @param secretKey AWS/MinIO secret key
     * @param region    AWS region (use "us-east-1" for MinIO)
     */
    public S3ConfigLoader(String endpoint, String bucket, String key,
            String accessKey, String secretKey, String region) {
        this(endpoint, bucket, key, accessKey, secretKey, region, 3, 200);
    }

    public S3ConfigLoader(String endpoint, String bucket, String key,
            String accessKey, String secretKey, String region,
            int maxAttempts, long initialBackoffMs) {
        this.bucket = bucket;
        this.key = key;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffMs = Math.max(50, initialBackoffMs);

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
                .apiCallAttemptTimeout(Duration.ofSeconds(5))
                .apiCallTimeout(Duration.ofSeconds(15))
                .build();

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .overrideConfiguration(overrideConfig)
                .forcePathStyle(true) // Required for MinIO
                .build();

        LOG.info("S3ConfigLoader initialized - endpoint: {}, bucket: {}, key: {}", endpoint, bucket, key);
    }

    /**
     * Loads the config file from S3.
     *
     * @return File contents as string
     */
    public String loadConfig() {
        try {
            LoadedObject loaded = loadObjectWithRetry();
            this.lastETag = loaded.eTag();
            this.lastContentHash = computeHash(loaded.content());

            LOG.info("Loaded config from s3://{}/{} (ETag: {}, size: {} bytes)",
                    bucket, key, lastETag, loaded.content().length());

            return loaded.content();
        } catch (Exception e) {
            LOG.error("Failed to load config from S3: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to load config from S3", e);
        }
    }

    /**
     * Checks if the config has changed since last load.
     * Uses ETag for quick check, falls back to content hash.
     *
     * @return true if config has changed
     */
    public boolean hasChanged() {
        try {
            HeadObjectResponse response = executeWithRetry(() -> {
                HeadObjectRequest request = HeadObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                return s3Client.headObject(request);
            }, "headObject");
            String currentETag = response.eTag();

            if (isEtagChanged(lastETag, currentETag)) {
                LOG.debug("Config changed - ETag mismatch: {} vs {}", lastETag, currentETag);
                return true;
            }

            if (shouldCheckHashFallback(lastETag, currentETag)) {
                String currentHash = fetchCurrentContentHash();
                if (isHashChanged(lastContentHash, currentHash)) {
                    LOG.debug("Config changed - content hash mismatch");
                    return true;
                }
            }

            return false;
        } catch (Exception e) {
            LOG.warn("Failed to check config changes: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Loads config only if it has changed.
     *
     * @return Updated config content, or null if unchanged
     */
    public String loadIfChanged() {
        if (hasChanged()) {
            return loadConfig();
        }
        return null;
    }

    /**
     * Gets the last loaded ETag.
     */
    public String getLastETag() {
        return lastETag;
    }

    /**
     * Gets the last content hash.
     */
    public String getLastContentHash() {
        return lastContentHash;
    }

    static boolean isEtagChanged(String previousETag, String currentETag) {
        return previousETag != null && currentETag != null && !previousETag.equals(currentETag);
    }

    static boolean shouldCheckHashFallback(String previousETag, String currentETag) {
        return previousETag == null || currentETag == null || previousETag.equals(currentETag);
    }

    static boolean isHashChanged(String previousHash, String currentHash) {
        return previousHash != null && currentHash != null && !previousHash.equals(currentHash);
    }

    private String computeHash(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            return null;
        }
    }

    private String fetchCurrentContentHash() {
        try {
            return computeHash(loadObjectWithRetry().content());
        } catch (Exception e) {
            LOG.warn("Failed to compute current content hash: {}", e.getMessage());
            return null;
        }
    }

    private LoadedObject loadObjectWithRetry() {
        return executeWithRetry(() -> {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            try (ResponseInputStream<GetObjectResponse> response = s3Client.getObject(request)) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = response.read(buffer)) != -1) {
                    baos.write(buffer, 0, bytesRead);
                }
                String content = baos.toString(StandardCharsets.UTF_8);
                return new LoadedObject(content, response.response().eTag());
            } catch (Exception e) {
                throw new RuntimeException("getObject read failed", e);
            }
        }, "getObject");
    }

    private <T> T executeWithRetry(Supplier<T> operation, String operationName) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.get();
            } catch (RuntimeException e) {
                last = e;
                if (attempt == maxAttempts) {
                    break;
                }
                long jitter = ThreadLocalRandom.current().nextLong(50);
                long delayMs = Math.min(5_000L, initialBackoffMs * (1L << (attempt - 1)) + jitter);
                LOG.warn("{} attempt {}/{} failed: {}. Retrying in {} ms",
                        operationName, attempt, maxAttempts, e.getMessage(), delayMs);
                sleep(delayMs);
            }
        }
        throw new RuntimeException(operationName + " failed after " + maxAttempts + " attempts", last);
    }

    private static void sleep(long delayMs) {
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record LoadedObject(String content, String eTag) {
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            LOG.info("S3ConfigLoader closed");
        }
    }
}
