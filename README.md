https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.nt.bz2

```
cargo build --release
# any other bzip decompressor will be a bottleneck
lbzcat /path/to/latest-truthy.nt.bz2 | ./target/release/wd-ent2ent
```

```
# if you want a progress bar, use pv
pv -d $(pidof lbzcat)
```