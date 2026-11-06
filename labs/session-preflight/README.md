# Get your two labs ready

> Added by Ryan Cicak on 2026-11-05. Revised 2026-11-06 to verify source versions, run both complete labs, and prepare their starting data.

Each hands-on participant runs [Session_Preflight.py](Session_Preflight.py) before the session, in the workspace we'll use together. Sign in as yourself, choose **Serverless**, select **Standard version 6** in the Environment panel, and click **Run all**.

The notebook downloads the exact session release and checks each file's fingerprint before importing it. It uses a new folder under your workspace home, leaving earlier notebook copies and edits in place.

## What it runs

After installing its client and restarting Python, the preflight starts two serverless job runs under your identity, both using environment version 6:

- [Branching](../branch-per-change/): build the inventory example, test an index on a branch, apply it to the lab's production branch, run the recovery exercise, and reset the starting data.
- [Retail Search](../retail-search-context/): load the products and inventory, run all eight answer checks, and verify the starting data and stock reset.

The optional project-move lab is separate and does not affect this result. There is no throwaway-project-only shortcut to READY.

## What READY means

Both labs completed successfully, returned the expected checks for this release and your identity, and verified their starting data. The final report includes the version, notebook links, project names, and a line to copy back to your Databricks contact.

A failed download, incomplete job, missing check, or failed reset prevents READY. Keep the error and job link if a run fails. The notebook needs permission to create notebooks in your home folder, start serverless job runs, and create and use the labs' Lakebase resources.

## Projects stay for the session

The two labs create or reuse their own projects and fictional data. Enabling Search in its dedicated project restarts compute and cannot be undone. The projects remain afterward, with a one-hour idle timeout, so you can use them during the session.

Preparation uses compute, and retained projects can incur charges. When finished with the labs, verify the project names and ownership before deleting them. Closing a notebook does not remove its project.

Run one preparation at a time per person and don't work in these projects while it runs. Different people can prepare at the same time. If the notebook says another preparation may still be active, check Jobs & Pipelines before retrying. Remove its preparation marker only after confirming those jobs have stopped.

On the day, open the notebook links from the successful report and run each lab from the top, one cell at a time. This preflight checks execution; the instructor still rehearses the explanation and pacing in the notebook UI.
