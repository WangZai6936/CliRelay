# StarBridge portal UI

The panel source is maintained upstream in `kittors/codeProxy`. The patch in
this directory keeps this repository's public account flow in the panel's
original component system:

- The ordinary-user login reuses `LoginPage` and its original layout, model
  animation, inputs, theme controls and network illustration.
- Registration uses the same panel components and returns to login with the
  username prefilled. It calls the backend's existing public signup API.
- Portal sign-out awaits session revocation, clears the local session and
  returns to `/login`, including when the server is unavailable.
- The shared wordmark, page metadata and mark use StarBridge branding.
- The login model showcase reads only the curated public list. System settings
  contains its versioned editor for names, built-in icons and display order.

The Docker frontend-source stage applies `0001-starbridge-portal.patch` before
building the panel. A failed patch application fails the build so an incompatible
upstream version cannot silently drop this flow.

For a separate frontend checkout, apply the patch with `git apply`, then run
`bun install --frozen-lockfile` and `bun run build`. Serve its `dist` directory
with this backend using `MANAGEMENT_PANEL_DIR`. The `/login` and `/register`
entry routes redirect into the built panel. Older unpatched panel releases do
not contain these account routes and must be rebuilt together with this backend.

Relevant frontend checks:

```sh
bun run --filter @code-proxy/admin-panel test PortalFlow.test.tsx LoginPage.test.tsx ApiKeyLookupPage.test.tsx
bun run build
```
