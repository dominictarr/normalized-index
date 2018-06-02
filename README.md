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

## notes

This design uses simple binary search on top of sorted pointer arrays indexes.
(aka, normalized-indexes) and then merges those, a la LSM trees.
So far I'm just using a simple binary search, and to merge,
using a slightly clever approach that merges really fast if there
are runs in the merge. If we are merging A and B, say we take ~N
from A and then ~M from B, this gets faster as N and M are bigger
as they get nearer to 1 it becomes just like merging two streams.
(except in the current implementation it doesn't degrade gracefully,
and does a log(N) lookup for each item - so that merging two
random streams is O(N*log(N)) which is worse than O(N). I'm think
of ways to improve this so that it handles both cases gracefully,
but the simplest right now is to avoid using this module for indexes
on unformly random values.

## License

MIT











