# Firefox use counters

This repo processes the use counters from public-data.mozilla.org and visualizes
them with a very simple [dashboard](https://mozilla.github.io/use-counters).

The data should be updated daily via GitHub actions (you can check the footer
for the actual date). There are various
[caveats](https://bugzilla.mozilla.org/show_bug.cgi?id=2019039#c1) (some use
counters are missing, some data is more aggregated than we may like).
