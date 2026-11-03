# Changelog

## 0.1.1a8+thalovant.1 — admission cache write is compare-and-set against revocation (2026-11-03)

- A lookup that misses the admission record reads the authoritative record and
  then writes `api_key_record:<key>`. An update or revocation committing
  between those two steps was undone by that write until the record expired.
  Every invalidation now also moves `api_key_epoch:<key>` (INCR + EXPIRE) in
  the same transaction, and the refill WATCHes it before its authoritative
  read and writes in MULTI/EXEC, so such a write aborts the refill instead.
  When the guard cannot be had, the lookup answers from the authoritative
  record and writes no cache. Legacy cluster mode never fills this cache and
  is unchanged; under a hash tag the epoch shares the record's slot. An
  operator that drops admission records must move the epoch too.
- The lookup logger falls back to the standard library logger when the
  logging facade lacks the attributes it reads, so the admission path cannot
  fail on a log line.
- The build carries the PEP 440 local label `+thalovant.1`.

## 0.1.1a8 — this fork brought up to the published release (2026-11-03)

The fork's `dev` was on the 0.1.0a7 line while every hub runs the published
0.1.1a8. The package (`hivemind_redis_database/__init__.py`, `migration.py`,
`version.py`), its tests, `pyproject.toml` and `README.md` are now that
release's files, byte for byte, except that `bitstring` is capped below 5
in the runtime dependencies (the bus client still imports `BitStream`) and
the README notes the cross-process delay of the opt-in in-process cache. The 0.1.0a7-line constraints are retired with the code
they described: the exact `filelock==3.20.3` and `hivemind-plugin-manager<0.9`
pins and their tests, and the hivemind-core 4.x end-to-end suite and its
workflow, which called a `crypto_key` client API that 5.x no longer has. The
runtime image that installs this package carries its own `filelock` and
`bitstring` constraints.

## [0.1.0a6](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a6) (2026-07-16)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a5...0.1.0a6)

**Merged pull requests:**

- Revert unauthorized automated merges \(\#31, \#32, \#33\) [\#35](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/35) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a5](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a5) (2026-07-16)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a4...0.1.0a5)

**Merged pull requests:**

- ci: release safely after fork merges [\#33](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/33) ([goldyfruit](https://github.com/goldyfruit))
- fix: advance last\_seen atomically [\#32](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/32) ([goldyfruit](https://github.com/goldyfruit))
- Use indexed API-key lookup and larger Redis pool [\#31](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/31) ([goldyfruit](https://github.com/goldyfruit))

## [0.1.0a4](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a4) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a3...0.1.0a4)

**Merged pull requests:**

- Update actions/checkout action to v7 [\#27](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/27) ([renovate[bot]](https://github.com/apps/renovate))

## [0.1.0a3](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a3) (2026-07-04)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a2...0.1.0a3)

**Merged pull requests:**

- test: hivescope e2e + CI [\#28](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/28) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a2](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a2) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.1.0a1...0.1.0a2)

**Merged pull requests:**

- docs: zero-to-hero README and /docs coverage [\#25](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/25) ([JarbasAl](https://github.com/JarbasAl))

## [0.1.0a1](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.1.0a1) (2026-06-05)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.4a2...0.1.0a1)

**Merged pull requests:**

- feat\(db\): schema v2 migration — fold legacy blacklist keys into metadata [\#20](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/20) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a2](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.0.4a2) (2026-05-20)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.4a1...0.0.4a2)

**Merged pull requests:**

- ci: pass PYPI\_TOKEN explicitly, drop secrets:inherit elsewhere [\#21](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/21) ([JarbasAl](https://github.com/JarbasAl))

## [0.0.4a1](https://github.com/JarbasHiveMind/hivemind-redis-database/tree/0.0.4a1) (2026-05-18)

[Full Changelog](https://github.com/JarbasHiveMind/hivemind-redis-database/compare/0.0.3...0.0.4a1)

**Merged pull requests:**

- Preserve client metadata \(supersedes \#17\) [\#18](https://github.com/JarbasHiveMind/hivemind-redis-database/pull/18) ([JarbasAl](https://github.com/JarbasAl))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
