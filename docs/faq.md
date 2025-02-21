# FAQ

#### What's different about TrinityLake compared with existing lakehouse formats like Apache Iceberg and Apache Hudi? Why creating another format rather than extending them?

TrinityLake is completely different from open table formats like Iceberg or Hudi. 
Trinitylake is NOT a table format. 
It focuses on 2 areas:

1. the definitions of different types of objects in a lakehouse
2. the interactions among the different objects in a lakehouse

That is also why there is a strong emphasis on features like transaction. 
For the table aspect of the project, it actually uses open table formats like Iceberg as a table format, 
and we are thinking about also extending the support to other ones like Hudi, Paimon, Lance, etc.

#### TrinityLake feels like a catalog, why not call it an "oepn catalog format"?

At this moment, we are focused more on the catalog side of features.
In fact, originally we wanted to call it an "open catalog format".
However, we felt this name is self-limiting the potential of the project to just do catalog things. 

The division of what belongs to a "table format" and what belongs to a "catalog" has been quite vague.
We don't really want to get too involved into the "feature ABC does not belong to a catalog" 
or "a catalog should behave like XYZ" types of debates.

By defining TrinityLake as a generic "open lakehouse format",
we reserve the freedom to do whatever that is beneficial for achieving our end goal:
to build a fully functional lakehouse with storage as the only dependency.

#### Why the name TrinityLake?

Trinity Lake, previously called Clair Engle Lake, is a beautiful reservoir on the Trinity River formed by
the Trinity Dam and located in Trinity County, California, USA.
Since we are building a format for Lakehouse, we decided to pick a name of a lake we like.

We also like the term _trinity_ as we see it symbolizes the 2 visions of this project:

1. build a lakehouse that brings catalog, table format, file format together to operate as one unified format on storage.
2. allow users to run analytics, ML & AI workloads all on the same lakehouse platform.
