# normalized-index

an database index for flumedb that only stores sequence/offset.
It's a [Log Structured Merge-tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree)
except it doesn't store keys, only pointers to the values which
are stored in the main flumelog.

This means that sorted data is fixed width, so is easy to binary
search. And most importantly, it means the indexes are very small.
If a 32 bit offset is used (for databases up to 4 gb) then an index
for a million records is only 4 mb, on the other hand, the size of a
denormalized LSM will depend on key size. Compound indexes (with more
than one key) are even more expensive for a denormalized index,
but the same cost for a normalized index.

The advantage of small indexes is that it becomes cheap to have
many indexes, 10-for-the-price-of-one.
This being a significant advantage for ad-hoc queries.

## motivation

This was the original idea that inspired [flumedb](https://github.com/flumedb)

For ssb

## License

MIT



