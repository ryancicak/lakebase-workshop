# Session Preflight

> **Modification notice:** Added by Ryan Cicak on 2026-11-05.

One notebook each hands-on person runs once, ahead of a session, in the workspace the session will use. It tells you
whether the labs will work there, installs them, and prints one line to send back. About five minutes.

## What to run

| Order | Open this | What it does |
|-------|-----------|--------------|
| 1 | [`Session_Preflight.py`](Session_Preflight.py) | Pick **Serverless**, then **Run all**. Four steps: a read-only quick check, the libraries the labs install, the labs installed as Git folders from public GitHub, and a full check with one throwaway project |

## The four steps

| Step | What it does | Changes anything? |
|------|--------------|-------------------|
| 1. Quick check | Serverless compute and its Python, protobuf, and SDK versions; network paths to PyPI and GitHub; Lakebase access and project headroom; whether the notebook sits in a Git folder | No |
| 2. Install | The same install cell the labs use: it keeps protobuf on the version the environment already ships | The notebook environment only |
| 3. Install the labs | Git folders under `/Users/<you>/lakebase-session-labs/` for this repository's lab branch and the optional move lab, or reuses them. Confirms both lab notebooks are present. Set `INSTALL_LABS = False` to skip | Folders in your own home folder only |
| 4. Full check | Creates one small project (`lb-preflight-...`, 1 CU), connects as you, writes a row, branches it, turns on Lakebase Search in it, tries a Unity Catalog schema (warning only), then deletes the project | One throwaway project, deleted before the cell ends |

It ends with a verdict per lab and one line that starts with `Lakebase preflight`. Every ❌ line says what to fix and
who can fix it.

## What it never touches

- Any existing project, folder, or table. The only projects it deletes are ones it created: name prefix
  `lb-preflight-`, display name "Lakebase session preflight (safe to delete)", owned by you, and older than 15 minutes
  if found as leftovers, so two copies running at once leave each other alone.
- Step 1 on its own is read-only.

## Related labs

- [Branch per Change](../branch-per-change/): branching the way a pull request uses it
- [Store-Aware Retail Search](../retail-search-context/): store context shaping a search result
