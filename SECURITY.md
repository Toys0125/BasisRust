# Security guidance

This repository has no published supported-release policy, dedicated security
contact or guaranteed response time. Check the repository's
[GitHub security page](https://github.com/Toys0125/BasisRust/security) for any
reporting mechanism enabled by the maintainers. If private vulnerability
reporting is available there, use it for sensitive details. Do not put credentials
or exploit details in public issues; a public issue can ask for a private
reporting route without disclosing them.

`default_password`, `ApiEnabled=false`, and the zero scene-relay limit are
compatibility defaults from BasisVR, not evidence of leaked operator secrets.
Set a non-empty deployment password on server and clients. The server supports
`BASIS_SERVER_PASSWORD` (preferred) and legacy `Password` environment overrides;
empty or non-Unicode password environment values stop startup. XML defaults and
the flat schema remain compatible. An explicitly empty XML password retains
legacy permissive behavior, so do not use it for protected deployments.

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
