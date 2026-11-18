# BlackShield Security Audit Report — `oxia-db/oxia`

- **Scanned Commit:** `6e6d087ac1a2f3c8a3a39b6ebba00e13387246b7`
- **Actionable Findings:** `1` (`0 Critical`, `1 High`, `40 Medium`, `0 Low`)
- **BlackShield Platform & Continuous Zero-Cost PR Protection:** [https://blackshield.chaplau.com](https://blackshield.chaplau.com)

## Detailed Findings & Remediation Guide

### 1. [HIGH] `CKV_DOCKER_1` — CKV_DOCKER_1: Container Runs as Root (Missing Non-Root USER)

- **Category:** `misconfiguration`
- **Location:** `Dockerfile:1`
- **Details:** Container does not specify a non-root USER, allowing processes to execute with root privileges.
- **Recommended Fix:** Add 'USER <non-root-uid>' instruction before container entrypoint.

