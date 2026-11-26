# Setup guide

## Part A: Upload to GitHub

### Option 1: Git command line (recommended)
1. Install Git and sign in to GitHub.
2. On GitHub, choose **New repository**. Name it `azure-databricks-retail-lakehouse`,
   set it to **Public**, and do **not** add a README, .gitignore or license.
3. Unzip the project, open a terminal inside the project folder, and run:
```bash
git init
git add .
git commit -m "Add retail sales lakehouse pipeline"
git branch -M main
git remote add origin https://github.com/<your-username>/azure-databricks-retail-lakehouse.git
git push -u origin main
```
If Git asks who you are, run `git config --global user.name "Your Name"` and
`git config --global user.email "you@example.com"`, then commit again.

### Option 2: Browser upload
1. Create the empty repository, then select **uploading an existing file**.
2. Drag in the **contents** of the project folder and commit.
3. Hidden items (`.github`, `.gitignore`) may be skipped by your file browser.
   Check the repository. If missing, use **Add file > Create new file**, type
   `.github/workflows/ci.yml` as the name, and paste the contents. Repeat for `.gitignore`.

### Check
- Open the **Actions** tab and confirm the **CI** run has a green tick.
  If it fails, open the log and fix the error before sharing the link.
- Confirm the README diagram renders.

## Part B: Run in Databricks
You can use an Azure Databricks workspace with Unity Catalog, or Databricks Free Edition
(no cost, serverless only, usage limits, non-commercial use). Free Edition is not an
Azure resource. On Azure, check the cost of your subscription before running anything.

1. In the workspace, open **Workspace > Create > Git folder** and paste your repository URL.
2. Open `notebooks/00_setup`, attach **Serverless** compute and run all.
   - `catalog` defaults to `workspace`. On Azure, enter a catalog that you can use
     (run `SHOW CATALOGS` to list them). You need permission to create schemas and volumes.
3. Run `01`, `02`, `03` and `04` in order. Use the same catalog and schema in each.
4. Compare row counts with the README.
5. Set `batch` to `2` in `00_setup`, run it, then run `01` to `04` again.
6. Run `sql/business_queries.sql` in the SQL editor (edit the `USE` line if needed).

To start over (**this deletes every table and file in the project schema**):
```sql
DROP SCHEMA workspace.retail_lakehouse CASCADE;
```

## Part C: Deploy the job (optional)
Install the current Databricks CLI, then from the project folder:
```bash
databricks auth login --host https://<your-workspace-url>
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run retail_lakehouse_job -t dev
databricks bundle run retail_lakehouse_job -t dev --params batch=2
```
For another catalog, add `--var="catalog=<name>"` to the validate, deploy and run commands.
You can also build the same five-task job manually in **Jobs & Pipelines**.

## Part D: Add proof of your run
1. Create a `docs/images` folder.
2. Take screenshots of the successful job or notebook run, the quarantine summary,
   a Gold query result and `DESCRIBE HISTORY silver_orders`.
3. Hide personal details such as your email and workspace URL.
4. Add them to the README, for example:
   `![Job run](docs/images/job_run.png)`
5. Commit and push.
