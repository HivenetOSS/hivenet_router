# Hivenet Router documentation instructions

## Scope

These instructions apply to every file under `docs/`.

This directory contains the Mintlify source for the Hivenet Router documentation published at [routerdocs.hivenet.com](https://routerdocs.hivenet.com).

- Documentation pages use MDX.
- Navigation, redirects, branding, and site settings live in `docs.json`.
- Images and logos live in `images/`.
- `README.md` contains contributor guidance for maintaining the documentation.

## Source of truth

Documentation must describe behavior implemented by the current repository source.

When sources disagree, use this order:

1. Current code and schemas
2. Current tests
3. Example configuration included in the repository
4. Root project documentation
5. Existing Mintlify documentation

Do not present planned, parsed-only, experimental, or partially implemented behavior as fully supported or enforced. State implementation boundaries explicitly when they matter to operators.

## Product and technical names

Use **Hivenet Router** as the public product name.

Use the exact identifiers implemented by the current source for:

- binaries and commands
- environment variables
- command-line flags
- API paths and request fields
- configuration keys
- metric names
- API-key prefixes
- protocol and service identifiers
- filesystem paths

Do not rename a technical identifier merely to make it resemble the public product name. Verify it in the current source first.

Use `hivenet-router`, `hivenet-agent`, `HIVENET_ROUTER_*`, and `sk-hivenet-*` where those names are implemented.

## Writing style

- Use direct, plain English.
- Use active voice and address the reader as “you.”
- Use sentence case for headings.
- Keep instructions concrete and operational.
- Define unfamiliar terms when they first appear.
- Use code formatting for commands, identifiers, paths, filenames, fields, and values.
- Use bold text for interface labels.
- Distinguish defaults from examples.
- Distinguish required settings from optional settings.
- State limitations near the feature they affect.

Avoid unsupported claims, promotional language, and invented examples that appear to be guaranteed behavior.

## Presentation system

Use Mintlify components to clarify structure, meaning, or choice. Do not add components only for decoration.

- Use `<Note>` for neutral supporting information that does not fit naturally in the main flow.
- Use `<Info>` for behavior, prerequisites, or caveats the reader must understand.
- Use `<Tip>` for optional shortcuts and improvements.
- Use `<Warning>` when ignoring the content can break a deployment, expose access, or produce an unsafe configuration.
- Use `<Danger>` only for destructive, irreversible, or security-critical actions.
- Use `<Check>` to confirm that a procedure or verification succeeded.
- Use `<Steps>` only for actions or events with a meaningful sequence.
- Use `<Tabs>` for mutually exclusive choices or parallel variants, not to hide prerequisite or safety information.
- Use `<CodeGroup>` for equivalent code or command alternatives.
- Use `<AccordionGroup>` for troubleshooting, edge cases, and secondary detail. Keep core instructions and limitations visible.
- Use `<Columns>` and `<Card>` for overview and navigation choices. Do not turn reference tables or ordinary paragraphs into cards.

Keep callout meaning consistent across pages. Core requirements must remain understandable when a reader scans only the headings and visible text.

## Page structure

Every MDX page must have valid frontmatter containing a clear title and description.

Prefer this general order when appropriate:

1. Purpose and scope
2. Prerequisites
3. Configuration or usage
4. Behavioral details
5. Verification
6. Troubleshooting
7. Related pages

Do not force this structure onto short reference pages where it does not help.

## Navigation and links

`docs.json` is the canonical navigation definition.

When adding, renaming, moving, or removing a page:

- update `docs.json`
- confirm every navigated path has a corresponding MDX file
- avoid duplicate navigation paths
- check for orphaned pages
- update incoming internal links
- add a permanent redirect from an established old URL when appropriate
- use canonical paths for new internal links instead of relying on redirects

Do not remove an existing redirect unless you have verified that it is obsolete.

## Assets

Preserve the Hivenet Router logo files, favicon, diagrams, and other existing assets unless the requested change explicitly replaces them.

Verify that every referenced asset exists and that its path uses the correct capitalization.

Do not recreate or rename brand assets without approval.

## Validation

Before considering a documentation change complete:

```bash
mint validate
mint broken-links
```

Also verify that:

- every navigated page exists
- navigation contains no duplicates
- intended pages are not orphaned
- redirect destinations exist
- internal links use valid paths
- images load
- MDX and frontmatter parse correctly
- files are saved as UTF-8
- examples match the current implementation
- no accidental placeholder or starter-kit content remains

For technically significant changes, compare the documentation directly with the relevant code, configuration structures, and tests.

## Git and review workflow

Never commit documentation changes directly to `main`.

1. Start from the latest `main`.
2. Create a dedicated branch.
3. Make and validate the changes.
4. Open a pull request.
5. Review both the GitHub diff and the Mintlify preview.
6. Wait for all required checks to pass.
7. Merge only after the repository owner explicitly approves the final version.

Do not merge a pull request merely because it is conflict-free or its automated checks pass.

If Mintlify’s web editor is used, confirm that it is writing to the review branch rather than directly to `main`.

Do not rewrite published branch history unless the repository owner explicitly approves it.

## Change reporting

Summaries must state:

- which pages or settings changed
- which technical sources were checked
- whether identifiers changed
- whether navigation or redirects changed
- what validation was performed
- any remaining limitations or manual work

If content was migrated, report exactly what was missing and what was restored.
