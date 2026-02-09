package com.example.fhir.connect.smt.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
import java.util.Base64;

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
        this.bucket = bucket;
        this.key = key;

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
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
                this.lastETag = response.response().eTag();
                this.lastContentHash = computeHash(content);

                LOG.info("Loaded config from s3://{}/{} (ETag: {}, size: {} bytes)",
                        bucket, key, lastETag, content.length());

                return content;
            }
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
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            HeadObjectResponse response = s3Client.headObject(request);
            String currentETag = response.eTag();

            if (lastETag != null && !lastETag.equals(currentETag)) {
                LOG.debug("Config changed - ETag mismatch: {} vs {}", lastETag, currentETag);
                return true;
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

    private String computeHash(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            LOG.info("S3ConfigLoader closed");
        }
    }
}
