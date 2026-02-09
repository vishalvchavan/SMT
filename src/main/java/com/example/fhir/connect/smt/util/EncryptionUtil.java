package com.example.fhir.connect.smt.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Utility class for AES-256-GCM encryption/decryption.
 * Provides secure encryption for sensitive field values.
 */
public class EncryptionUtil {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12; // 96 bits
    private static final int GCM_TAG_LENGTH = 128; // 128 bits

    private final SecretKey secretKey;
    private final SecureRandom secureRandom;

    /**
     * Creates an EncryptionUtil with the given base64-encoded key.
     * Key must be 32 bytes (256 bits) for AES-256.
     *
     * @param base64Key Base64-encoded 32-byte key
     */
    public EncryptionUtil(String base64Key) {
        byte[] keyBytes = Base64.getDecoder().decode(base64Key);
        if (keyBytes.length != 32) {
            throw new IllegalArgumentException("Encryption key must be 32 bytes (256 bits). Got: " + keyBytes.length);
        }
        this.secretKey = new SecretKeySpec(keyBytes, "AES");
        this.secureRandom = new SecureRandom();
    }

    /**
     * Encrypts a plaintext string using AES-256-GCM.
     * Output format: Base64(IV + ciphertext + tag)
     *
     * @param plaintext The text to encrypt
     * @return Base64-encoded encrypted string
     */
    public String encrypt(String plaintext) {
        if (plaintext == null || plaintext.isEmpty()) {
            return plaintext;
        }

        try {
            // Generate random IV
            byte[] iv = new byte[GCM_IV_LENGTH];
            secureRandom.nextBytes(iv);

            // Initialize cipher
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);

            // Encrypt
            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));

            // Combine IV + ciphertext
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);

            return Base64.getEncoder().encodeToString(combined);
        } catch (Exception e) {
            throw new RuntimeException("Encryption failed", e);
        }
    }

    /**
     * Decrypts a Base64-encoded encrypted string.
     *
     * @param encryptedBase64 Base64-encoded encrypted string
     * @return Decrypted plaintext
     */
    public String decrypt(String encryptedBase64) {
        if (encryptedBase64 == null || encryptedBase64.isEmpty()) {
            return encryptedBase64;
        }

        try {
            byte[] combined = Base64.getDecoder().decode(encryptedBase64);

            // Extract IV
            byte[] iv = new byte[GCM_IV_LENGTH];
            System.arraycopy(combined, 0, iv, 0, iv.length);

            // Extract ciphertext
            byte[] ciphertext = new byte[combined.length - iv.length];
            System.arraycopy(combined, iv.length, ciphertext, 0, ciphertext.length);

            // Initialize cipher
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);

            // Decrypt
            byte[] plaintext = cipher.doFinal(ciphertext);
            return new String(plaintext, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Decryption failed", e);
        }
    }

    /**
     * Generates a new random 256-bit key as Base64 string.
     * Use this to create a new encryption key.
     *
     * @return Base64-encoded 32-byte key
     */
    public static String generateKey() {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return Base64.getEncoder().encodeToString(key);
    }
}
