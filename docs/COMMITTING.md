# Committing and landing

How changes reach `main`. The git-keeper runs the landing pass; authors run their own branches and PRs.

## Authors: code changes

1. Commit on your own branch. Never commit on `main`.
2. Push the branch and open the PR as a draft (`gh pr create --draft`).
3. Tell the reviewer when the PR is open, so review can start early.
4. The guard reviews the patch. Its PASS is recorded for the head commit.
5. Watch CI on the head sha (`gh pr checks --watch`) and fix failures until it is green.
6. When CI is green and review is done, run `gh pr ready`, then tell the git-keeper.

A PR that is still a draft is never landed.

## Git-keeper: landing

The landing pass (`tmp/lvs-land-ready.sh`) lands a PR only when all of these hold:

- the PR is not a draft and its base is `main`;
- every check on the head sha is green;
- the guard PASS is recorded for that head sha;
- the head is a fast-forward of `origin/main`;
- no commit in the range has a co-author or AI trailer.

Rules for landing:

- Fast-forward only. No merge commits and no GitHub merge button. The owner is the committer.
- A PR that is not a fast-forward is rebased by its author, then checked again.
- PRs stacked on a branch that lands are retargeted to `main` first.
- The landed head branch is deleted on `origin`.
- CI on the exact head sha is the verification. The keeper does not rebuild locally.
- A skipped PR gets its reason from the landing log, sent to the author through lv-super.

## Dependabot

Dependabot PRs follow the same rules: green CI, then landed by the git-keeper. The keeper does not edit the bot's branches by hand.

## Commit messages

- Subject line in the form `area: what changed`, then a body that says why.
- No `Co-Authored-By` lines and no AI attribution, in commits or in PR bodies.
- Commits are authored and committed by the owner's configured git identity.

## Docs

Documentation changes follow the same flow as code. The doc-keeper owns `README.md` and `docs/`, except `docs/COMMITTING.md`, which the git-keeper owns.

## Releases

There are no release tags for now. The `edge` image is built from `main` by the publish workflow, after the end-to-end tests pass. Users pin `ghcr.io/kirmorozov/lazyview:edge` by digest, not by tag.
