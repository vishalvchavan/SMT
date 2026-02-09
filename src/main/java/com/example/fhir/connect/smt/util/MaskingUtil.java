package com.example.fhir.connect.smt.util;

import java.util.regex.Pattern;

/**
 * Utility class for masking sensitive data.
 * Supports various patterns like SSN, credit card, email, phone.
 */
public class MaskingUtil {

    private MaskingUtil() {
        // Utility class
    }

    /**
     * Masks a value based on the specified pattern type.
     *
     * @param value       The value to mask
     * @param patternType The type of masking pattern (ssn, creditcard, email,
     *                    phone, custom)
     * @param customMask  Custom regex pattern (only used when patternType is
     *                    "custom")
     * @return Masked value
     */
    public static String mask(String value, String patternType, String customMask) {
        if (value == null || value.isEmpty()) {
            return value;
        }

        return switch (patternType.toLowerCase()) {
            case "ssn" -> maskSSN(value);
            case "creditcard", "credit_card", "cc" -> maskCreditCard(value);
            case "email" -> maskEmail(value);
            case "phone" -> maskPhone(value);
            case "name" -> maskName(value);
            case "custom" -> maskCustom(value, customMask);
            case "full" -> maskFull(value);
            case "partial" -> maskPartial(value);
            default -> maskPartial(value);
        };
    }

    /**
     * Masks SSN: 123-45-6789 → ***-**-6789
     */
    public static String maskSSN(String ssn) {
        if (ssn == null || ssn.length() < 4) {
            return "****";
        }
        // Remove any formatting
        String cleaned = ssn.replaceAll("[^0-9]", "");
        if (cleaned.length() >= 9) {
            return "***-**-" + cleaned.substring(cleaned.length() - 4);
        } else if (cleaned.length() >= 4) {
            return "***-**-" + cleaned.substring(cleaned.length() - 4);
        }
        return "****";
    }

    /**
     * Masks credit card: 4111111111111111 → ****-****-****-1111
     */
    public static String maskCreditCard(String cardNumber) {
        if (cardNumber == null || cardNumber.length() < 4) {
            return "****";
        }
        // Remove any formatting
        String cleaned = cardNumber.replaceAll("[^0-9]", "");
        if (cleaned.length() >= 12) {
            return "****-****-****-" + cleaned.substring(cleaned.length() - 4);
        } else if (cleaned.length() >= 4) {
            return "****-" + cleaned.substring(cleaned.length() - 4);
        }
        return "****";
    }

    /**
     * Masks email: john.doe@example.com → j***@example.com
     */
    public static String maskEmail(String email) {
        if (email == null || !email.contains("@")) {
            return "****@****.***";
        }
        int atIndex = email.indexOf('@');
        String localPart = email.substring(0, atIndex);
        String domain = email.substring(atIndex);

        if (localPart.length() <= 1) {
            return "*" + domain;
        }
        return localPart.charAt(0) + "***" + domain;
    }

    /**
     * Masks phone: 555-123-4567 → ***-***-4567
     */
    public static String maskPhone(String phone) {
        if (phone == null || phone.length() < 4) {
            return "****";
        }
        // Remove any formatting
        String cleaned = phone.replaceAll("[^0-9]", "");
        if (cleaned.length() >= 10) {
            return "***-***-" + cleaned.substring(cleaned.length() - 4);
        } else if (cleaned.length() >= 4) {
            return "***-" + cleaned.substring(cleaned.length() - 4);
        }
        return "****";
    }

    /**
     * Masks name: John Smith → J*** S***
     */
    public static String maskName(String name) {
        if (name == null || name.isEmpty()) {
            return "****";
        }
        String[] parts = name.split("\\s+");
        StringBuilder masked = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0)
                masked.append(" ");
            if (parts[i].length() > 0) {
                masked.append(parts[i].charAt(0)).append("***");
            }
        }
        return masked.toString();
    }

    /**
     * Custom masking using regex replacement.
     * The customMask should be in format: "regex|replacement"
     * Example: "\\d|*" replaces all digits with *
     */
    public static String maskCustom(String value, String customMask) {
        if (customMask == null || !customMask.contains("|")) {
            return maskPartial(value);
        }
        String[] parts = customMask.split("\\|", 2);
        try {
            Pattern pattern = Pattern.compile(parts[0]);
            return pattern.matcher(value).replaceAll(parts[1]);
        } catch (Exception e) {
            return maskPartial(value);
        }
    }

    /**
     * Full masking: completely replaces value with asterisks.
     */
    public static String maskFull(String value) {
        if (value == null)
            return null;
        return "*".repeat(Math.min(value.length(), 16));
    }

    /**
     * Partial masking: shows first and last character, masks middle.
     * Example: "sensitive" → "s*******e"
     */
    public static String maskPartial(String value) {
        if (value == null)
            return null;
        if (value.length() <= 2) {
            return "*".repeat(value.length());
        }
        return value.charAt(0) + "*".repeat(value.length() - 2) + value.charAt(value.length() - 1);
    }
}
