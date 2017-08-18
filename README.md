# Dooby

This is toy "database" (it's just a an in-memory map). I made it in order to play around with the idea of transactions as a persistent data structure. 

A transaction (in this context) chains together reads and writes (linked list) that can be checked for consistency with data in the DB at the time of the commit. If the data is not consistent, it means some value that the transaction has read is now out of date. In this case the logic that produced the transaction can be retried until its entire span is based on a consistent view of the data.
