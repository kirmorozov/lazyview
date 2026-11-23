# Upgrading

Every behaviour change an operator must act on, one section per release, newest first, each line
saying what to do. The sections are written **as the PRs land**, not at tag time: a PR that
changes operator-visible behaviour carries its own line here. The GitHub Release body for a tag
**links the section for that release** instead of restating it, so nobody has to keep two copies
of the same text.

A version named below is the release the change arrives in. *v0.2* is the release that makes the
definitions table read-only for the service
([server setup](server-setup.md#advanced-setup-recommended-for-production)); `Cargo.toml` still
says `0.1.0` and no tag beyond `v0.1.0` exists, so until that release is tagged these are the
planned names.

## Upgrading from v0.1 to v0.2

* Rebuild requests are a counter: use `UPDATE _lazyview SET rebuild = rebuild + 1`.
  `SET rebuild = 1` works only once per view
  ([rebuilding a view](views.md#rebuilding-a-view)).
* Run `lazyview setup` with the setup account before starting the v0.2 service. It widens
  `_lazyview.rebuild` to `BIGINT UNSIGNED` and adds `_lazyview_state.rebuild_served`; the
  operations run checks both and exits `2` naming the setup run until they exist
  (`ensure` and `verify`, `src/meta.rs:384-411` and `:286`). The widening is an `ALTER TABLE` on
  your definitions table, so on a large `_lazyview` plan the upgrade like any `ALTER TABLE`.
* A request the previous release left unserved is served once after the upgrade: `rebuild = 1`
  with a new `rebuild_served` row at `0` is a pending request. Expect one rebuild per view that had
  a flag set when you stopped the old service, and no others.
* The service no longer writes any column of `_lazyview`, so it needs no write right there. Revoke
  what v0.1 needed —
  `REVOKE UPDATE (name, rebuild) ON lazyview._lazyview FROM 'lazyview_svc'@'%';` —
  and keep its `SELECT` ([the accounts](server-setup.md#advanced-setup-recommended-for-production)).
* `name` is no longer filled in from a view's `CREATE VIEW` statement: it is your column now, and
  a definition that is a bare `SELECT` fails until you set it (``the definition needs a name: set
  `_lazyview.name`, or write CREATE VIEW name AS SELECT …``, `src/analyze/parse.rs:54`). Rows
  written on the old release already carry the name their statement gives them, so they keep it; a
  row you add whose definition is a bare `SELECT` needs `name` from you.
* To put the definitions in a schema of their own, set `LAZYVIEW_DEFINITIONS_DATABASE` for **both**
  runs and grant the service account `SELECT` in that schema and nothing else; the DDL rights there
  belong to the setup account
  ([definitions in a schema of their own](server-setup.md#definitions-in-a-schema-of-their-own)).
