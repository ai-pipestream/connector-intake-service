package io.pipeline.connector.util;

import com.password4j.Argon2Function;
import com.password4j.Hash;
import com.password4j.Password;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.security.SecureRandom;
import java.util.Base64;

/**
 * Utilities for API key generation and hashing.
 * <p>
 * API keys are:
 * <ul>
 *   <li>Generated as secure random Base64 strings</li>
 *   <li>Hashed with Argon2id before storage (current best practice)</li>
 *   <li>Never stored in plaintext</li>
 *   <li>Only returned once at creation/rotation time</li>
 * </ul>
 * <p>
 * Uses password4j library with Argon2id algorithm, which provides:
 * <ul>
 *   <li>Resistance to GPU cracking attacks</li>
 *   <li>Protection against side-channel attacks</li>
 *   <li>Memory-hard algorithm (configurable memory cost)</li>
 *   <li>Winner of Password Hashing Competition</li>
 * </ul>
 */
@ApplicationScoped
public class ApiKeyUtil {

    private static final Logger LOG = Logger.getLogger(ApiKeyUtil.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final int API_KEY_BYTES = 32; // 256 bits

    // Argon2id parameters (balanced security/performance for API keys)
    // These match OWASP recommendations for 2024
    private static final int MEMORY_COST = 65536;  // 64 MB
    private static final int ITERATIONS = 3;        // Time cost
    private static final int PARALLELISM = 4;       // Number of threads
    private static final int HASH_LENGTH = 32;      // 256 bits output

    private static final Argon2Function ARGON2 = Argon2Function.getInstance(
        MEMORY_COST,
        ITERATIONS,
        PARALLELISM,
        HASH_LENGTH,
        com.password4j.types.Argon2.ID  // Argon2id variant (hybrid mode)
    );

    /**
     * Generate a new secure random API key.
     * <p>
     * Generates a cryptographically secure random byte array and encodes
     * it as Base64. The result is URL-safe and suitable for HTTP headers.
     *
     * @return Base64-encoded API key (plaintext - must be hashed before storage)
     */
    public String generateApiKey() {
        byte[] randomBytes = new byte[API_KEY_BYTES];
        SECURE_RANDOM.nextBytes(randomBytes);
        String apiKey = Base64.getUrlEncoder().withoutPadding().encodeToString(randomBytes);
        LOG.debugf("Generated new API key (length: %d)", apiKey.length());
        return apiKey;
    }

    /**
     * Hash an API key for secure storage.
     * <p>
     * Uses Argon2id with the following parameters:
     * <ul>
     *   <li>Memory cost: 64 MB</li>
     *   <li>Iterations: 3</li>
     *   <li>Parallelism: 4 threads</li>
     *   <li>Hash length: 256 bits</li>
     * </ul>
     * <p>
     * The hash includes a randomly generated salt and is stored in PHC string format.
     *
     * @param plaintextApiKey The plaintext API key
     * @return Argon2id hash in PHC format suitable for database storage
     */
    public String hashApiKey(String plaintextApiKey) {
        Hash hash = Password.hash(plaintextApiKey).with(ARGON2);
        String hashString = hash.getResult();
        LOG.debugf("Hashed API key with Argon2id (hash length: %d)", hashString.length());
        return hashString;
    }

    /**
     * Verify an API key against a stored hash.
     * <p>
     * Uses constant-time comparison to prevent timing attacks.
     * Automatically detects the hashing algorithm from the PHC format.
     *
     * @param plaintextApiKey The plaintext API key to verify
     * @param storedHash The Argon2id hash from database (PHC format)
     * @return true if the key matches the hash, false otherwise
     */
    public boolean verifyApiKey(String plaintextApiKey, String storedHash) {
        try {
            boolean matches = Password.check(plaintextApiKey, storedHash).with(ARGON2);
            LOG.debugf("API key verification: %s", matches ? "SUCCESS" : "FAILED");
            return matches;
        } catch (Exception e) {
            LOG.warnf(e, "API key verification failed with exception");
            return false;
        }
    }
}

