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
- tests
- explicit sources
- an explicit "project"
- profiles
- per-developer namespacing
- templating & macros
- seeds
- packages
- "ephemeral" models
- many supported platforms


## Things Waterwheel had that DBT doesn't
- state
- multi-statement builds


## Areas where Waterwheel and DBT made different choices
- how tables are created
- how dependencies are declared/inferred
- config
- selector features
- "incremental"
- naming / project directory layout
