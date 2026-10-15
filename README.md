# CommerceFlow — Session 2 Event Streaming Console

A Python **Tkinter** desktop GUI closely modeled on the seven-tab screenshots: Pipeline, Durable log, Consumers & lag, Failure & recovery, Replay, Reconciliation, Console.

## Input data
Uses the uploaded five CommerceFlow Session 1 CSV datasets (bundled in `Datasets/`). The Session 2 transaction stream uses **all 300,000 rows of the orders CSV**, matched to customer **state** from CRM; unknown locations are labeled `Unknown`. The durable log and Kafka payload retain each source order's ID, customer/product IDs, raw amount, date, payment method, status, and quantity. CRM state is the only joined value. Dirty source strings and values are retained rather than replaced with assumed categories. The separate derived revenue metric follows the Session 1 normalization rule: only successful orders with positive, parseable amounts contribute revenue. Replay groups by the source `payment_method` value.

## Run

1. Extract ZIP (keep the `Datasets` folder beside `session2_gui.py`).
2. Install Python 3.10+ (Tkinter is normally bundled on Windows).
3. Install dependencies with `python -m pip install -r requirements.txt`.
4. Open the project-root `.env` file and enter your PostgreSQL login:

   ```dotenv
   PGHOST=localhost
   PGPORT=5432
   PGUSER=your_postgres_username
   PGPASSWORD=your_postgres_password
   ```

   The app always targets database `commerceflow`. `.env` is excluded by `.gitignore`; do not share or commit the file after adding your password. Existing Windows environment variables take precedence over `.env`. The app will show a clear error in the Pipeline notice and Console if credentials, database access, or permissions are unavailable.

5. Double-click `START_SESSION2_GUI.bat` or run `python session2_gui.py`.
6. Click **Run everything** for production, all three consumer groups, batch reconciliation, and replay. Or run stages separately. Click the other tabs to inspect results.

## Architecture and fidelity notes

- **PostgreSQL-backed:** The GUI connects directly to database `commerceflow`. It creates and writes only `session2_events`, `session2_offsets`, `session2_audit`, and `session2_alerts` in the current schema; it does not truncate or alter Session 1 tables. **Produce** resets only these Session 2 tables and reloads all 300,000 dataset rows.
- **Consumers and recovery:** Three independent named consumer offsets and persistent PostgreSQL side-effect tables. Failure simulation rolls back uncommitted writes and resumes from committed PostgreSQL offsets.
- **Optional Kafka export:** Install `kafka-python` (included in requirements), start Kafka at `localhost:9092`, create topic `transaction.recorded` with **4 partitions**, then click **Publish to Kafka**. This sends the PostgreSQL event rows using `region` as the message key. The GUI's consumers continue to consume from PostgreSQL, not Kafka; its throughput is not a Kafka benchmark.
- **Reconciliation:** Performs a fresh aggregate from all input CSV orders, then compares exactly matching region sets and counts, per-region mean revenue within 1e-6, and final lag zero.
- Output artifacts in `results/`: `session2_throughput.csv`, `consumer_lag.csv`, `streamed_regional_revenue.csv`, `reconciliation_report.json`, `replay_revenue.csv`.
- Rate measurements refer to PostgreSQL operations; they are not Kafka broker throughput.

## Important data distinctions

The screenshot's exact distribution, throughput and revenue values were recorded from a *different event-streaming example*; this app computes new values from the uploaded CSVs. The sample's raw `order_date` formats are mixed/dirty: replay's optional start-date field uses lexical string comparison; leave it blank for full-history replay. Time-based replay should be normalized first for a production system.
