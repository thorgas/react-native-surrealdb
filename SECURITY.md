# Security Policy

## Supported versions

`react-native-surrealdb` is currently an early alpha. Security fixes are made
only on the current `main` branch and, after publication, the latest alpha
release. Older commits and alpha releases may not receive fixes.

Persistent SurrealKV support is experimental. Applications must validate their
own encryption, access control, backup, recovery, migration, and device-loss
requirements before storing sensitive data.

## Reporting a vulnerability

Do not open a public issue containing vulnerability details, credentials,
private databases, production data, or unredacted logs.

Use GitHub's **Report a vulnerability** button on the repository's Security
page when it is available. If private vulnerability reporting is not available,
open a minimal issue asking the maintainer to establish a private contact
channel; include no exploit details in that issue.

In a private report, include:

- the affected package version or commit;
- the affected platform, React Native version, and connection mode;
- impact and realistic attack prerequisites;
- minimal reproduction steps or a proof of concept;
- suggested mitigations, if known; and
- whether the issue has been disclosed elsewhere.

The maintainer aims to acknowledge reports within seven days, then confirm
scope, coordinate a fix and disclosure timeline, and credit the reporter unless
anonymity is requested. Please allow a reasonable remediation period before
public disclosure.

## Scope

Security-sensitive areas include the Rust/JSI boundary, value decoding, query
and authentication handling, embedded storage, transaction and live-query
lifecycle, WebSocket transport, native packaging, and the release pipeline.
General bugs, performance regressions, and unsupported configuration questions
belong in the public issue tracker once secrets and personal data are removed.
