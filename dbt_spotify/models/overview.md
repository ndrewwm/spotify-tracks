# dbt Docs: `spotify-tracks`

This is the auto-generated docs page for the `dbt_spotify` project within the [spotify-tracks](https://github.com/ndrewwm/spotify-tracks) repo. Here you can browse descriptions of the models built by the project, and view their interdependencies in the site's *lineage graph*.

I've attempted to use consistent naming conventions and proper data modeling techniques. *Staging* tables/views are prefixed with `stg_`, *dimensions* with `dim_`, *facts* are prefixed with `fct_`, and *reports* are prefixed with `rpt_`.

## Navigation

You should primarily use the `Project` tab. All the models are built within a single database and schema. Within the **Projects** tab, the folder structure of the source code is replicated. You'll be able to look at each of the models in `/staging/` and `/marts/`.

## Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of models within the project.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the `--select` and `--exclude` model selection syntax to filter the models in the graph. For more information on model selection, check out [dbt's docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.
