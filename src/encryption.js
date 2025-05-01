export async function getEncryptionKey(password) {
  // Initialize encryption key from password
  const encoder = new TextEncoder();
  const passwordData = encoder.encode(password);
  
  // Derive a key from the password
  const keyMaterial = await crypto.subtle.importKey(
    "raw",
    passwordData,
    "PBKDF2",
    false,
    ["deriveBits", "deriveKey"]
  );
  
  // Use PBKDF2 to derive a key
  return await crypto.subtle.deriveKey(
    {
      name: "PBKDF2",
      salt: encoder.encode("wa-sqlite-encrypted-vfs"),
      iterations: 100000,
      hash: "SHA-256"
    },
    keyMaterial,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}
