# Firefox use counters

This repo processes the use counters from public-data.mozilla.org and visualizes
them with a very simple [dashboard](https://mozilla.github.io/use-counters).

The data should be updated daily via GitHub actions (you can check the footer
for the actual date). There are various
[caveats](https://bugzilla.mozilla.org/show_bug.cgi?id=2019039#c1) (some use
counters are missing, some data is more aggregated than we may like).

## Local development

To serve the site locally, you first need to populate `static/data/` with the
JSON data files, then run a local HTTP server from the `static/` directory.

**If you want to change the aggregation code**, run the full pipeline from raw
telemetry (slow, downloads ~GBs of data):

```
cargo run --release -- aggregate -o static/data -c ./cache
```

**If you only want to work on the front-end**, mirror the pre-built data from
the live site instead (fast):

```
cargo run --release -- artifact -o static/data
```

Once the data is in place, serve the site with any HTTP server, e.g.:

```
python -m http.server -d static/
```
