Pipeline currently just ETLs everything and adds on top, potentially creating
a larger and larger file. Truncate and insert can also be done if we only want 
the latest info (although snapshots may be required - what is the purpose??)

Arbitrary decisions made based on ease of obtaining stuff due assessment just
purely being on coding skill; the asking questions is more important

Better coding needed if we want to do the snapshot linking on the pipeline, but
time constraints, best to just join on latest
