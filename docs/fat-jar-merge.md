# Fat-jar merge audit (pipeline-scala)

`sbt assembly` merges 24 runtime jars into `pipeline-scala.jar`. The merge
policy in `pipeline-scala/build.sbt` is explicit; every path not matched by a
rule uses `deduplicate`, which **fails the build** if two jars contain the
same path with different bytes.

Audit of sbt-assembly 2.2.0 debug output (2026-10-23). Counts are *source
entries* (one per jar that contains the path), matching sbt's own summary
line.

| Strategy | Entries | What |
|---|---|---|
| `deduplicate` | 570 | 282 `.class` files under `shaded/parquet/it/unimi/dsi/fastutil/**`, each present byte-identically in both `parquet-column-1.14.4.jar` and `parquet-hadoop-1.14.4.jar` (564 entries); 3 non-class `META-INF` files byte-identical in `jackson-core-2.17.0.jar` and `parquet-jackson-1.14.4.jar` (6 entries) |
| `discard` | 268 | 234 `META-INF/maven/**` (embedded `pom.xml`/`pom.properties`, build metadata); 24 `META-INF/MANIFEST.MF`; 10 `module-info.class`; 0 signature files |
| `rename` | 29 | `META-INF/LICENSE*` (17) and `META-INF/NOTICE*` (12): legal notices kept per source jar (e.g. `META-INF/LICENSE_parquet-jackson-1.14.4`) |
| `filterDistinctLines` | 6 | `META-INF/services/com.fasterxml.jackson.core.JsonFactory`, `META-INF/services/com.fasterxml.jackson.databind.Module` |
| `concat` | 0 | no `reference.conf` / `application.conf` present |

**Content conflicts between `.class` files: none.** The rule was introduced
after an initial `deduplicate`-everything build failed only on legal notices
and embedded Maven metadata.

To reproduce the audit:

```sh
cd pipeline-scala
sbt -batch 'set assembly/logLevel := Level.Debug' clean assembly > assembly-debug.log
```
