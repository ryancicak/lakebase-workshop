# Branch per change

Copy-on-write database branches, used the way a pull request would use them. One notebook, about 45 minutes
hands-on, plus an optional recovery block of about three minutes.

## What to run

| Order | Open this | What you'll do |
|-------|-----------|----------------|
| 1 | [`Branch_Per_Change.py`](Branch_Per_Change.py) | Create your own project, load a two-million-row production-shaped table, branch it in seconds, reproduce a slow lookup on the branch and fix it there with an index, prove production never moved, promote the same change, diff the two, delete the branch. Optional: an agent runs a DELETE without its store filter against production, the stores keep working, and you restore a point in time and merge only the deleted rows back, with anything newer in production winning |

## Prerequisites

- Permission to create a Lakebase project in the workspace (every workspace user has it by default).
- Serverless compute for the notebook. Pick **Serverless** in the compute menu, then **Run all**.

There is no setup notebook to run first. The first cell creates your project (`lakebase-lab-<you>`) or reuses it.

## What it creates, and what it never touches

- One project named `lakebase-lab-<you>` with a one-hour idle timeout, one schema, one table (`store_inventory`), and
  short-lived branches (`pr-101`, and in the optional block `recovered`).
- Nothing outside that project. Every cell can be re-run; step 2 resets the story (drops the promoted index, removes
  leftover branches) so the notebook tells the same story every time.
- The project stays at the end so you can run it again. The last cell, commented out, deletes it.

## Key concepts

- **Copy-on-write branching**: a branch has every row instantly; nothing is copied until one side changes.
- **Branch per change**: the branch is where the change is proven, against the full data, before production sees it.
- **Promotion by replaying the change**: the branch never merges into production; the same statement runs on production.
- **Point-in-time restore**: a branch of production exactly as it was at any second in the history window. Use it two
  ways: re-point the app at it, or copy the deleted rows out of it and merge them back into production by key with
  anything newer in production winning, which is what the optional block does. Snapshots are the scheduled version;
  a checkpoint branch is the manual one.

## Documentation

- [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches)
- [Manage branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches)
- [Backup and restore methods](https://docs.databricks.com/aws/en/oltp/projects/backup-methods)
- [Point-in-time restore](https://docs.databricks.com/aws/en/oltp/projects/point-in-time-restore)
