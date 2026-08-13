# Hivenet Router documentation

This directory contains the source for the Hivenet Router documentation site:

**[routerdocs.hivenet.com](https://routerdocs.hivenet.com)**

The documentation is built with [Mintlify](https://www.mintlify.com). Pages are written in MDX, while navigation, branding, redirects, and site-wide settings are defined in [`docs.json`](docs.json).

## Directory structure

- `index.mdx` contains the documentation homepage.
- `getting-started/` contains introductory and architectural material.
- `deploy/` contains router and agent deployment guides.
- `use-the-api/` documents the supported HTTP APIs.
- `routing/` covers policies, fallback behavior, gates, and admission control.
- `security/` documents authentication, API keys, restrictions, and rotation.
- `observability/` covers metrics, dashboards, audit logs, and operational signals.
- `integrations/` contains client and application integration guides.
- `reference/` contains detailed configuration, architecture, errors, and performance information.
- `project/` contains contributing, governance, security, and licensing pages.
- `images/` contains documentation images and logo assets.
- `docs.json` defines navigation and permanent redirects.

## Preview locally

The Mintlify CLI requires Node.js 20.17 or newer.

Install or update the CLI:

```bash
npm install -g mint@latest
```

From this directory, start a local preview:

```bash
cd docs
mint dev
```

The preview is available at:

```text
http://localhost:3000
```

You can also run the preview without installing the CLI globally:

```bash
cd docs
npx mint dev
```

## Validate changes

Before opening or merging a pull request, run:

```bash
mint validate
mint broken-links
```

Confirm that:

- every page listed in `docs.json` exists
- renamed pages retain permanent redirects
- internal links use canonical page paths
- images and other assets load correctly
- MDX files contain valid frontmatter
- text is saved as UTF-8
- technical names match the current router source

## Documentation conventions

Use **Hivenet Router** as the public product name.

Use the exact names implemented by the current source for binaries, environment variables, API paths, configuration fields, metrics, key prefixes, and other compatibility identifiers. Do not rename a technical identifier solely to make it match the public product name.

When behavior changes, verify the documentation against the current implementation before describing it as supported or enforced.

Keep page titles and headings in sentence case. Prefer active voice, direct instructions, and concrete examples.

## Contribution workflow

Documentation changes follow the same review process as code changes:

1. Start from the latest `main`.
2. Create a dedicated branch.
3. Edit the MDX pages and `docs.json`.
4. Preview and validate the site locally.
5. Open a pull request.
6. Review the GitHub diff and Mintlify preview.
7. Merge only after the review is complete and all required checks pass.

If you use Mintlify’s web editor, confirm that it is committing to the review branch rather than directly to `main`.

Changes merged into `main` are deployed automatically to [routerdocs.hivenet.com](https://routerdocs.hivenet.com).
