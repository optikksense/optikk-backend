## Full-Stack Consistency Rule

When making **any** change — a feature, fix, refactor, or removal — always audit and update **both** frontend and backend before considering the task done.

### Adding or Modifying
- If adding a new API endpoint → ensure the frontend calls it correctly (update fetch/axios calls, types, hooks, etc.)
- If adding a new UI feature → ensure the backend has the supporting route, validation, and data model
- If changing a field name, data shape, or contract → update both the API response **and** the frontend consumer
- If adding auth/permissions logic → enforce it on the backend **and** reflect it in the UI (hide/show, disable, redirect)

### Removing
- If removing a backend endpoint → remove or update every frontend call to that endpoint
- If removing a UI component or page → remove the corresponding backend route/handler if it exists solely for that feature
- If removing a data field → strip it from the API response, DB schema, **and** all frontend references (display, forms, types)

### Checklist (run mentally before marking anything done)
- [ ] Backend change made?
- [ ] Frontend updated to match?
- [ ] Shared types / interfaces / contracts in sync?
- [ ] No dead frontend calls to removed/renamed endpoints?
- [ ] No orphaned backend routes with no frontend consumer?
