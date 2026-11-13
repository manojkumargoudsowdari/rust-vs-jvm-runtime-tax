# Project overview in plain words

This page explains what this project is for without technical background.
For the design, see [PLAN.md](../PLAN.md). For running on AWS, see
[aws.md](aws.md).

## The goal: lower the cost of computing

Companies pay cloud providers for every machine they rent to process data.
Many data jobs are written in Java or Scala, which run on the JVM (Java
Virtual Machine). Rust is a newer language that runs directly on the
hardware, with no virtual machine in between.

**The question:** if a company rewrites a data job from Java/Scala in Rust,
does its cloud bill go down, and by how much?

## An analogy: two delivery vans

Two vans carry the same parcels to the same addresses.

- **The JVM van** has a built-in helper who rides along: the JVM runtime.
  The helper does useful things, such as tidying up memory (garbage
  collection) and finding faster routes on the fly (JIT compilation). But
  the helper takes up seats (memory) and burns fuel (CPU).
- **The Rust van** has no helper. The route was worked out before leaving
  (the program is compiled ahead of time), so the van is smaller and uses
  less fuel.

Both vans deliver exactly the same parcels: every benchmark run checks that
the Rust and Scala programs produce bit-identical output. The project
measures how much extra the helper costs. That extra is the **runtime tax**.

## Where the money goes

| What you rent in the cloud | Why the JVM may cost more | What the project measures |
|---|---|---|
| Memory | The JVM reserves extra memory for its heap and its own machinery, so a job may need a bigger, pricier machine | Peak memory, and the smallest heap each garbage collector needs |
| CPU time | Time spent on garbage collection and compilation is CPU you pay for but that does no business work | CPU-seconds and garbage-collection pauses |
| Time | Slower jobs keep machines rented for longer | Wall-clock time and per-batch latency |
| Machine size | Memory and CPU together decide which machine size you must rent | Cost per run, priced with real AWS rates |

## What the results will let us say

Once the measurements exist, the paper can make statements of this form:

- "For this kind of job, Rust needs X% less memory, so it fits on a smaller
  machine."
- "It uses Y% less CPU, so the same machines can process more data."
- "It costs Z dollars less per amount of data processed."

X, Y and Z come only from measurements on real machines; none exist yet.
The answer could go either way. If the runtime tax turns out to be small,
that is still a useful finding: a rewrite may not be worth the money.

## Where the measurements run

| Machine | Role | Why |
|---|---|---|
| A local PC or laptop running Ubuntu natively | Free practice: set up, debug and rehearse every step | Mistakes are fixed without paying for cloud time. Its numbers are not used in the paper. |
| AWS `c6id.4xlarge` | Main results | A virtual machine of the kind companies actually rent, so its prices are real-world prices |
| AWS `c6id.metal` | Cross-check | The same family of hardware without virtualization, to confirm the virtual machine does not distort the comparison |

Energy use is not the main goal. Lower CPU and memory use usually means
lower energy too, so energy may be added later as a side result.

## One-sentence summary

> We measure how much extra cloud money is paid for running data jobs on the
> JVM compared with Rust (the runtime tax in memory, CPU and machine size),
> using identical workloads and real AWS pricing.
