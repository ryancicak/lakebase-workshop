# Branch per change

> Added by Ryan Cicak on 2026-11-05. Session preparation added for release 2026-11-06d.

Test an index on a separate database branch, check the result, then apply the same SQL to production.
Plan for a 60-minute block: about 45 minutes for setup and the index exercise, up to 10 minutes for optional
recovery, and five minutes to discuss where this fits your change process.

## What to run

| Order | Open this | What you'll do |
|-------|-----------|----------------|
| 1 | [`Branch_Per_Change.py`](Branch_Per_Change.py) | Create your project and sample inventory, test an index on `pr-101`, confirm production is unchanged, then add the tested index to production and delete the branch. Optional: recover deleted inventory rows while keeping newer store updates |

## Prerequisites

- Permission to create a Lakebase project in the workspace.
- Serverless compute for the notebook. Pick **Serverless**, then run **one cell at a time from the top**.

There is no separate setup notebook. Setup A through E install libraries and load helpers; step 1 creates
your project (`lakebase-lab-<you>`) or reuses it. Each executable cell has a short **What**, **Why**, and **Look for**
introduction. Pause at each result before continuing. If Python restarts, rerun Setup D and E, then restart the exercise.

An existing project is reused only when its owner matches your signed-in identity. If ownership is missing or
different, the notebook stops before changing the project.

Steps 1 through 9 are the main lab. Recovery is split into five cells so the room can follow the deleted rows,
newer work, restore, and repair. If time is short, skip all of step 10 and go to Wrap-up. This notebook demonstrates
the database operations manually; it does not create a pull request or run GitHub Actions.

## Preparing before the session

The session preparation notebook can run this entire lab in serverless environment version 6, including recovery.
It then removes the added SKU index, repair table, and extra SKU. It verifies two million inventory rows,
the original primary key, and only the production branch remain. Your project and data stay available.

This mode requires the matching release and a preparation run ID. It returns a structured result only after the
exercise, recovery, and reset checks pass. The preparation notebook checks that result before reporting READY.

During a normal cell-by-cell run, the preparation reset is skipped and your completed lab stays in place.
Start from the top on the day so Python loads the libraries and helpers; step 2 resets the exercise as usual.

## What it creates, and what it never touches

- One project named `lakebase-lab-<you>` with a one-hour idle timeout, one schema, one table (`store_inventory`), and
  short-lived branches (`pr-101`, and in the optional block `recovered`).
- `production` is a branch inside your lab project. The stores and products are made up.
- To repeat the exercise, start at step 1 and run step 2 to reset the added index, extra SKU, and temporary branches.
- The project stays at the end so you can run it again. The last cell, commented out, deletes it.

## Key concepts

- **Copy-on-write branching**: a new branch shares unchanged data with its source. Changes on either branch are separate.
- **Branch per change**: the branch is where the change is proven, against the full data, before production sees it.
- **Promotion by replaying the change**: the branch never merges into production; the same statement runs on production.
- **Point-in-time restore**: create a separate branch at an earlier time within the available history window. The
  optional exercise copies candidate rows from that branch and inserts only keys missing from production.
  `ON CONFLICT DO NOTHING` keeps existing production rows. It does not compare timestamps or resolve arbitrary updates.

## Documentation

- [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches)
- [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Backup and restore methods](https://docs.databricks.com/aws/en/oltp/projects/backup-methods)
- [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
