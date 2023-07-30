The original Waterwheel was built during 2017-18 at a company that no longer exists. The source code is lost, but there's a lot I still remember because it was Mine and I put my heart and soul into it. Memory is imperfect of course, and some details may be inaccurate.

It feels important to note tht DBT was open-sourced by Fishtown Analytics (now DBT Labs) after Waterwheel had been conceived and mostly built. Waterwheel and DBT were very much aimed at the same problem and their authors made many very similar design decisions, but they did so completely independently.

Waterwheel was written in Python 3 and leaned heavily on Luigi and networkx.

Waterwheel itself was developed separately from the set of scripts that were given to it as input. DBT works identically in this respect.

Waterwheel was distributed as a Docker image. This was not _strictly_ necessary but getting (and keeping) Python installed and working on BI developers' machines was seen as Highly Undesirable. I'm not sure it's any easier now than it was then, but attitudes have shifted. DBT provides no self-contained packaging and expects users to `pip install` it... or use DBT Cloud.

Waterwheel was heavily wired into our data lake, and particular environmental details and design decisions. Because it incorporated so many assumptions about its environment, even if the source code had not been lost it would not be feasible to simply _use_ Waterwheel the way one uses DBT. It was never built to be portable to other environments. It was, to use Rich Hickey's term, a "situated program".

One specific assumption Waterwheel made that would likely be unpopular elsewhere is that there is a single, shared development database. Contrast this with DBT's approach of developer-specific namespaces.

As mentioned earlier, Waterwheel used Luigi, and in fact Waterwheel's CLI was built around Luigi's CLI generation. To invoke Waterwheel was to invoke a Luigi Task. Waterwheel exposed 4 such Tasks as its top-level interface:
- `TableBuild`
- `BuildEverything`
- `Downstream`
- `Upstream`

An example invocation would be something like
`$ waterwheel TableBuild --local-scheduler --schema warehouse --table fact_customer_order --build-id noah20230717-01`
or
`$ waterwheel BuildEverything --build-id main-build-20230717 --daydate 2017-07-17 --workers 8`

`TableBuild` would build a single table, without any regard for dependencies. This was the primary way a developer would interact with a particular table they were making or modifying. Equivalent to `dbt run --model foo`

`BuildEverything` would build every table defined in the script repo, in dependency-aware order. Equivalent to `dbt run`

`Downstream` and `Upstream` would take the task graph of `BuildEverything` and narrow it to only the descendants or ancestors of a specific table, plus that table. Equivalent to the dbt selectors `foo+` and `+foo`, respectively. This feature of Waterwheel was inspired by Drake, and based on the selector syntax I assume that was DBT's inspiration as well.

It is unfortunate that the detail of `--local-scheduler` was exposed to users, and I was displeased with this even at the time. The difficulty was that connecting to a central Luigi scheduler was the default behavior, and specifying `--local-scheduler` was required to run fully locally. That's what you always wanted for development, but for production runs we _did_ use a central scheduler. Fixing this in a wrapper script shouldn't have been difficult, but to the best of my recollection we simply lived with this.

Two non-obvious parameters you may have noticed were:
- `--daydate`
- `--build-id`

The `daydate` parameter was part of how Waterwheel was integrated with our data lake. It was a somewhat overloaded concept and did several things
- For tables extracted from service DBs, each day's extraction was labeled with a `daydate` value such that the snapshot _taken on_ YYYY-MM-DD had `daydate=YYYY-MM-DD`. These values were used to check the freshness of sources.
- For tables that _accumulated_ append-only data, the data that _belonged to_ YYYY-MM-DD would have `daydate=YYYY-MM-DD`
- The value of `daydate` passed to Waterwheel as a parameter (with a default of "today" if not given) was made available in the `_globals` temp table so that Waterwheel build scripts could use it as desired

`build_id` was more interesting. It's important to understand Luigi's approach to determining which Tasks need to run, which is too large a topic to cover here in detail but in short is very `make`-like. Like `make`, Luigi has the concept of a Target, and if the output Target of a Task exists, that Task will not run. This is how Luigi avoids repeating work while a developer is working, or in case of a crash. A Luigi Target is more abstract than a `make` target, in that it need not be a file, but for simplicity Waterwheel did in fact use files. The function of `build_id` was to _namespace_ those files, in order to give the developer control over what counts as "this run" when it comes to tracking progress.

For example, let's say you had the simple task graph A->B->C->D, and ran the whole thing. On your first run A, B, and C succeeded, but D failed. Re-running the very same command _would start from D_, because A, B, and C each indicated their completion by creating a Target file. But what if you wanted to build them over again? One option would have been to acquaint developers with the specifics of target files so that they could delete those files when needed. I did not see this as desirable. Instead, I used the value of `build_id` to namespace those target files, so that getting a "clean" build state required only passing a "new" value to `--build-id`. This was still somewhat difficult to explain to less technical users (even this explanation feels inadequate), but we were all able to make it work in practice by following simple rules of thumb like "use a new build id for each table you're working on" and "when in doubt, choose a new build id". Definitely an area for improvement.


## Things DBT has that Waterwheel didn't
- docs
Waterwheel's approach to docs was "if you write a README.md in your table directory, Bitbucket will render it" (yes we used Bitbucket not Github). For our purposes that was fine. DBT obviously goes much further, with structured table- and field-level docs, that it renders as an interactive site, including an interactive graph that supports selector syntax.

- tests
Tests are something I struggled with, conceptually, for a long time while working on Waterwheel. For a true **test**, you would want to run a build in some kind of alternate or temporary environment where you can feed it known data for all of its input tables so that you can validate its output. For various reasons owing to the nature of SQL and databases, this is hard. I eventually punted entirely, vowing to return to the problem in the future, which sadly never happened.

DBT's approach is not really _testing_ but rather _validation_. That is, it doesn't feed your code known inputs to be validated against expected outputs, rather it runs your code against whatever input is available then check broad assertions about the results. In this way it is quite similar to Great Expectations, so it's not all that clear that "having 'testing' built-in" is a major value-add.

See ON_UNIT_TESTING_SQL.md for more.

- explicit sources
I chose to assume that any declared dependency for which there was no build script _must_ be a source. This worked out fine, but I think my view has shifted since then towards "explicit sources are good".

- an explicit "project"
DBT's `dbt_project.yml` holds certain top-level configuration values that Waterwheel never needed. This is mainly because DBT attempts to be much more general and configurable, so that it can be adapted to local needs.

- profiles
As with the concept of a "project", profiles are an approach to configuration that Waterwheel simply didn't need. Because it was fully built into its environment, the nature of "production" and "development" environments were assumed and built in, rather than being configurable.

- per-developer namespacing
Related to "profiles", DBT favors an approach where each developer gets their own namespace. For example if your production output schema is "analytics", Alice's developer profile might place her outputs in "analytics_alice". This is surely intended to avoid conflicts and concurrency issues, or more broadly to maintain the illusion that developers have a "local" environment. My approach with Waterwheel went the other way, embracing a shared development environment, because shared data is simply the reality of what we're doing. If Alice and Bob are working on the same table, it's _better_ that they bump into each other while working rather than later when they try to commit/merge. Sharing data in development also meant that devs never had to build from scratch, because we ran an automated dev build each morning, the output of which was available for everyone's use. Bootstrapping one's DBT namespace is a constant problem.

I see this as an area where reasonable people can disagree. There are _tradeoffs_ in electing shared vs personal development data, and those tradeoffs vary with many attributes of your environment and team (team _size_ being only the most obvious). And ideal tool would support both strategies and document suggested workflows for each.

- templating & macros
These are extremely bad.
See ON_TEMPLATING_SQL.md for much more.

- seeds
In general I prefer to keep data in a database, rather than in source control. Because of that, the idea of seeds would likely never have occurred to me. I can see why you might want to, even though I disagree. That said, DBT's implementation of seeds sucks. CSV only? Terrible.

- packages
Packages as a way of distributing modular collections of macros (ugh) or tests is a reasonable idea, and in practice _everyone_ uses `dbt-utils`. But packages as a way of distributing collections of _models_ is insane. It may seem like "models for Salesforce data" or "models for Zendesk data" would be portable, but they are not. **All** data transformation logic is idiosyncratic to a particular organization. In any event, as with projects and profiles, this was an idea Waterwheel simply didn't need.

- "ephemeral" models
I still have no idea what problem ephemeral models are meant to solve. Best as I can tell, they should never be used.

- many supported platforms
Waterwheel was built exclusively for Redshift since that's what we used. A few details of how Redshift works, and of how psycopg2/libpq works, leaked into its implementation. DBT of course strives to support a variety of popular data warehouse platforms.


## Things Waterwheel had that DBT doesn't
- state
This is a big one. One of the two main **purposes** for using a DAG framework is to avoid repeating work that has already been done (or, looked at from a different perspective, to recover from partial failure). DBT does not support this at all (certainly not when it was new, and best as I can tell still does not), whereas Waterwheel supported this as a first-class feature (see the discussion of `--build-id`).

It is vital, in my view, that in a scenario where you execute a build of 100 tables, which results in 99 successes and 1 failure, that you have the power to fix the failure, re-run the build, and see it skip the 99 things it's already done. If you don't see why that's essential, we live on different planets. Building a data warehouse invariably _takes significant wall-clock time_, often measured in hours. When you experience failures, which are more often due to external factors than they are to "a DW dev made a mistake", Waterwheel is there to help you, where DBT is not. The DBT developers either don't see this as a problem (again: different planets), or believe that it's up to developers/operators to use selectors to trim the build when recovering.

DBT has introduced a concept of state -- see https://docs.getdbt.com/reference/node-selection/methods#the-state-method -- but it seems not to do what I'm describing here. It seems to be more concerned with the evolution of _project_ state.

- multi-statement builds
DBT mostly requires a model to be specified as a single SQL statement, which must be a query as it is then wrapped in a CTAS. This has the advantage of enforcing that a model script builds the model it says it does and nothing else. Beyond that though, it is extremely limiting. What if you want to create a temp table? What if your model covers 3 cases that are best expressed separately? What if you need to do an `update`? Or call a procedure? The DBT authors seem to believe that "more models" or "do a union" are always acceptable answers. Waterwheel treats you like an adult.

It's telling that DBT actually provides an escape hatch here, at least with BigQuery, that lets you write a multi-statement script.

To be clear, single-statement scripts are worth preferring, and having that as a goal is fine. But we don't always exist on the happy path, and developers should be empowered to solve problems as they see fit.


## Areas where Waterwheel and DBT made different choices
- how tables are created
This is arguably my biggest disagreement with DBT's design, and the one I estimate as least likely to ever be resolved to my satsifaction.

The proper first step in designing a table is to write a `create table` statement. Not because you "have to", but because it forces you to **design** what you are building before you set about building it. You must choose a name, a primary key, field names and types and nullity. If your platform has other concepts such as storage engine, data distribution, sorting, compression, and so on, this is also when you must consider those factors.

DBT not only doesn't force you to do this, it **deprives** you of the opportunity to do it. Because DBT wraps your models in CTAS, you **cannot** specify types, nullity, compression codecs, and so on. It provides configuration elements to handle some of these issues (partitioning in BQ, distribution in Redshift, etc), but leaves you helpless to specify types and nullity, perhaps the most fundamental field-level choices you have to make (beyond names).

It _might_ be possible to solve this deficiency of DBT using a "custom materialization", but frankly that may not be worth it, because it would put you so far outside the normal use of the tool.

- how dependencies are declared/inferred
- config
- selector features
- "incremental"
- naming / project directory layout
