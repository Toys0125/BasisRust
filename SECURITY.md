# Security guidance

This repository has no published supported-release policy, dedicated security
contact or guaranteed response time. Private vulnerability reporting is currently
disabled in the repository. Check the
[GitHub security page](https://github.com/Toys0125/BasisRust/security) for changes
and use GitHub's **Report a vulnerability** flow if it becomes available. Until
then, do not put credentials, exploit details or other sensitive information in a
public issue. Open a minimal public issue asking maintainers for a private contact
route without describing the vulnerability. There is no guaranteed response time.

## Dependency audits

PR CI audits both Cargo lockfiles. It narrowly defers RustSec advisories
RUSTSEC-2026-0194 and RUSTSEC-2026-0195 for quick-xml 0.38.4: the repository
uses it to parse operator-controlled configuration and resource XML, and no
`NsReader` is used for network input. Updating to quick-xml 0.41 requires API
migrations in the client and permissions parsers, which are maintained in a
separate workstream. CI does not suppress other advisory IDs; remove this
deferral when those parser migrations land. The audit still displays the current
unsoundness warnings for `anyhow` and `tokio`.

`default_password`, `ApiEnabled=false`, and the zero scene-relay limit are
compatibility settings from BasisVR, not evidence of leaked operator secrets.
The server no longer selects `default_password` automatically: new configs have
authentication enabled and no password, so startup fails until an operator
supplies a non-empty secret. Setting `Password` explicitly to `default_password`
remains possible for compatibility, but should be limited to isolated parity
testing. To intentionally run a public/passwordless server, explicitly set
`UseAuth=false`. Set a non-empty deployment password on server and clients. The server supports
`BASIS_SERVER_PASSWORD` (preferred) and legacy `Password` environment overrides;
empty or non-Unicode password environment values stop startup. XML fields and
the flat schema remain compatible. An empty XML password now stops startup
whenever authentication is enabled.

See [configuration and deployment](BasisRustServer/README.md#configuration) for
precedence, file paths and persistence. Environment settings avoid committing
secrets, but are visible to administrators of the host/container. `/config save`
explicitly writes the effective config, including environment-supplied secrets.
Keep that file private and untracked.

The server uses upstream-compatible UDP and Ed25519 identity authentication;
password authentication does not encrypt traffic. Health endpoints have no
authentication and default to loopback. Exposing them requires an operator
network/access-control decision. API enablement and any scene-relay bandwidth
limits are explicit operator settings; this guidance does not change defaults
or constitute a cryptographic audit.
