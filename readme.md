This repository contains a single blueprint (but I'll add more) showcasing the use of [Kiba ETL](https://www.kiba-etl.org) and [Kiba Pro](https://github.com/thbar/kiba/wiki#kiba-pro).

Note that Kiba Pro is a commercial extension ; running `bundle install` requires an active subscription. Configuration instructions can be found [here](https://github.com/thbar/kiba/wiki/How-to-install-Kiba-Pro).

License for this repository: MIT.

### Postgres bulk upsert (ON CONFLICT UPDATE)

A table is initially created, with a `product_ref` column with a unicity constraint.

Records are generated in memory using an enumerable, transformed to build rows as `Hash` instances, then upserted into the target table.

The destination is configured to:
- Insert the row if none exist with the same `product_ref`
- If a row exists with the same `product_ref`, update its price, quantity and description

Processing occurs in batches of 25k records, grouped as a single transactions.

A little `DSLExtension` is used to inject some form of feedback and measure.
