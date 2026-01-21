# Code Signing and Notarization Guide

This guide details the process for signing and notarizing Vertebra single-file executables for macOS and Windows.

## macOS (Code Signing & Notarization)

To distribute binaries on macOS without triggering Gatekeeper warnings, you must sign your application with an Apple Developer ID certificate and notarize it with Apple.

### Prerequisites

1.  **Apple Developer Account:** Enrolled in the Apple Developer Program ($99/year).
2.  **Certificates:** `Developer ID Application` certificate generated in your Apple Developer account.
3.  **Tools:** `rcodesign` (Rust-based) or Xcode's `codesign` and `notarytool`. We recommend `rcodesign` for cross-platform signing (e.g., signing macOS binaries on Linux CI).

### Workflow (using `rcodesign`)

1.  **Sign the Binary:**
    Sign the executable produced by `vertebra build --one-file`.

    ```bash
    rcodesign sign \
      --p12-file path/to/certificate.p12 \
      --p12-password "your-password" \
      --code-signature-flags runtime \
      dist/myapp-darwin-arm64
    ```
    *Note: The `--code-signature-flags runtime` flag is required for the Hardened Runtime, which is a prerequisite for notarization.*

2.  **Zip the Binary:**
    Apple's notary service expects a zipped package or disk image.

    ```bash
    zip myapp.zip dist/myapp-darwin-arm64
    ```

3.  **Submit for Notarization:**
    Upload the zip file to Apple.

    ```bash
    rcodesign notary-submit \
      --api-key-path path/to/app-store-connect-api-key.json \
      --max-wait-seconds 3600 \
      myapp.zip
    ```

4.  **Staple the Ticket:**
    Once notarization succeeds, staple the ticket to your executable so it works offline.

    ```bash
    rcodesign notary-staple dist/myapp-darwin-arm64
    ```

### Workflow (using Xcode tools on macOS)

1.  **Sign:**
    ```bash
codesign --force --options runtime --sign "Developer ID Application: Your Name (TEAMID)" dist/myapp-darwin-arm64
    ```

2.  **Zip:** `zip myapp.zip dist/myapp-darwin-arm64`

3.  **Notarize:**
    ```bash
xcrun notarytool submit myapp.zip --keychain-profile "AC_PASSWORD" --wait
    ```

4.  **Staple:** `xcrun stapler staple dist/myapp-darwin-arm64`

---

## Windows (Code Signing)

To prevent "Unknown Publisher" warnings and SmartScreen blocks, sign your `.exe` with a Microsoft Authenticode certificate.

### Prerequisites

1.  **Code Signing Certificate:** Purchased from a CA like DigiCert, Sectigo, or GlobalSign. EV (Extended Validation) certificates provide instant SmartScreen reputation but are more expensive.
2.  **Tools:** `signtool.exe` (Windows SDK) or `osslsigncode` (Cross-platform).

### Workflow (using `osslsigncode` on Linux)

1.  **Sign the Binary:**

    ```bash
    osslsigncode sign \
      -pkcs12 path/to/certificate.p12 \
      -pass "your-password" \
      -n "My App" \
      -i "https://example.com" \
      -t http://timestamp.digicert.com \
      -in dist/myapp-windows-x64.exe \
      -out dist/myapp-windows-x64-signed.exe
    ```
    *Note: Always use a timestamp server (`-t`) so the signature remains valid after the certificate expires.*

### Workflow (using `signtool` on Windows)

1.  **Sign:**
    ```powershell
signtool sign /tr http://timestamp.digicert.com /td sha256 /fd sha256 /f path/to/certificate.p12 /p "password" dist/myapp-windows-x64.exe
    ```

## CI Integration

Both workflows can be automated in GitHub Actions.

**Secrets Required:**
- `APPLE_CERT_P12_BASE64`
- `APPLE_CERT_PASSWORD`
- `APPLE_API_KEY_JSON`
- `WINDOWS_CERT_P12_BASE64`
- `WINDOWS_CERT_PASSWORD`

**Example Step (macOS):**
```yaml
- name: Sign macOS binary
  run: |
    echo "${{ secrets.APPLE_CERT_P12_BASE64 }}" | base64 --decode > cert.p12
    rcodesign sign ... --p12-file cert.p12 ...
```

