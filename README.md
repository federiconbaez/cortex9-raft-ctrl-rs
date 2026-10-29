# cortex9-raft-ctrl-rs

Raft that survives corrupted disks: CTRL protocol-aware recovery (FAST'18) under deterministic simulation.

> **ES** abajo · **EN** below

## ES

### Qué es

Un KV replicado con Raft cuyo log vive en un dispositivo de sectores simulado que puede corromper sectores en silencio. Hay dos políticas sobre el mismo núcleo Raft:

- **Vanilla**: cuando una entrada falla el checksum, el nodo reinicia y trunca el log desde esa entrada, igual que con una cola rota por un crash. Este es el comportamiento que pierde datos confirmados.
- **CTRL** (Alagappan et al., *Protocol-Aware Recovery for Consensus-Based Storage*, FAST'18):
  - Cada entrada tiene un registro de identificador (índice, término, CRC) en otra región del dispositivo. Ese registro distingue una cola rota (sin identificador, nunca confirmada, se descarta) de una corrupción (con identificador, se conserva).
  - La entrada corrupta mantiene su lugar en el log y se sigue contando en las elecciones, así que un candidato al que le falta no gana.
  - Un seguidor la recibe de nuevo del líder.
  - El líder la pide a los demás (`FixReq`/`FixResp`). Solo la descarta si una mayoría responde que no la tiene y su índice está por encima del commit; después deja el término.

El simulador inyecta corrupción de sectores, crashes, colas rotas y pérdida y reordenamiento de mensajes. Cada corrida se juzga con tres chequeos:

- El checker de linealizabilidad estilo Porcupine de [`cortex9-raft-sim`](https://github.com/federiconbaez/cortex9-raft-sim), usado como dependencia fijada a `0b37dc1`.
- Un oráculo de seguridad de la máquina de estados (dos réplicas que aplican cosas distintas en el mismo índice).
- El conteo de truncamientos por debajo del commit.

### Resultados medidos

Máquina: VM Linux 6.12, 8 vCPU Intel Xeon, rustc 1.99.1, build release. Los números salen del binario compilado en el commit `143a9a8`. Los commits posteriores solo tocan CI, docs y el número de rondas de un test bajo Miri.

| Criterio | Comando | Resultado | Estado |
|---|---|---|---|
| 10k semillas, corrupción en minoría, CTRL sin violaciones | `run --mode ctrl --faults minority --seeds 10000 --require-safe --min-recovery 0.99` | 0 violaciones, 0 indecididas; 114 283 sectores corruptos inyectados, 113 210 reparados | PASS |
| Vanilla pierde datos con las mismas fallas | `run --mode vanilla --faults minority --seeds 10000 --require-violation` | 1 232 de 10 000 semillas con violación; 4 817 870 entradas truncadas por corrupción | PASS |
| Recuperación automática ≥ 99% con corrupción en minoría | la misma corrida de CTRL | 10 000 de 10 000 semillas recuperadas (100,00%) | PASS |
| Overhead sin fallas ≤ 10% | `overhead --seeds 1000 --reps 5 --max-overhead 0.10` | CPU +3,10% (mediana de 5); throughput 0,00%; latencia 0,00%; bytes en disco +6,25% | PASS |

Controles extra:

| Control | Comando | Resultado |
|---|---|---|
| CTRL con todas las copias corruptas | `run --mode ctrl --faults majority --seeds 2000 --require-safe` | 0 violaciones; 0 de 2000 recuperadas (se queda sin disponibilidad, como corresponde) |
| Control negativo: descartar con una sola respuesta "no la tengo" | `run --mode ctrl --faults majority --seeds 2000 --bug discard-on-one-lack --require-violation` | 53 de 2000 semillas con violación: el checker lo detecta |
| 3 nodos, CTRL | `run --mode ctrl --nodes 3 --seeds 2000 --require-safe --min-recovery 0.99` | 0 violaciones; 2000 de 2000 recuperadas |
| 3 nodos, vanilla | `run --mode vanilla --nodes 3 --seeds 2000 --require-violation` | 1 119 de 2000 con violación |

Cómo se define cada número:

- **Corrupción en minoría**: para cada índice del log, como mucho ⌊(n−1)/2⌋ réplicas reciben alguna vez una corrupción en ese índice. Las fallas son un bit invertido o un sector puesto en cero, con sesgo hacia las últimas entradas y a veces seguidas de un crash.
- **Recuperada**: al final de la corrida se cumplen tres cosas. Ninguna réplica tiene entradas corruptas. Cada réplica aplicó todo lo comprometido antes del fin de la fase de fallas. Hubo operaciones de clientes completadas después del reinicio rodante, que obliga a cada réplica a releer su log. El denominador son las semillas con al menos una corrupción inyectada.
- **Overhead**: corridas sin corrupción (con crashes, colas rotas y pérdida de mensajes) en los dos modos con las mismas semillas.
  - Sin fallas el protocolo es idéntico, así que operaciones completadas y latencia simulada coinciden exactamente.
  - La diferencia de CPU es el costo de escribir y verificar los registros de identificador: tiempo de pared de la simulación completa, mediana de 5 repeticiones intercaladas.
  - En bytes, CTRL agrega 32 bytes por entrada de 512 (6,25%).

### Limitaciones (honestas)

- Es una simulación determinista, no un sistema distribuido real. No hay disco real ni `fsync`. El overhead de un disco real (orden de escrituras entre el sector y su identificador) **no está medido**.
- Los metadatos (término y voto) no se corrompen en este modelo. CTRL los guarda redundantes y aquí se asumen sanos.
- Se asume que una falla no corrompe a la vez el sector de una entrada y su registro de identificador. Es la misma hipótesis del paper.
- Sin snapshots ni cambios de membresía.
- "Vanilla" modela una política concreta: truncar en la primera entrada mala. Otros sistemas reales hacen otras cosas, como caerse o ignorar el checksum.
- La recuperación de 100% depende de una fase de curación de 1500 ticks sin fallas. Con corrupción en una mayoría de copias de una entrada comprometida, CTRL no recupera: queda sin disponibilidad y no pierde datos.
- El CI no corrió todavía en GitHub: el repositorio solo existe localmente.

### Uso

```sh
cargo test
cargo build --release
./target/release/raft-ctrl-rs run --mode ctrl --faults minority --seeds 10000 --require-safe --min-recovery 0.99
./target/release/raft-ctrl-rs run --mode vanilla --faults minority --seeds 10000 --require-violation
./target/release/raft-ctrl-rs overhead --seeds 1000 --reps 5 --max-overhead 0.10
```

Los escenarios guiados en `tests/scenarios.rs` muestran el caso central paso a paso:

1. Una escritura confirmada en {A, B} con C aislado.
2. La copia de B se corrompe y A cae.
3. En vanilla, B trunca, vota por C y la escritura desaparece.
4. En CTRL, B conserva el identificador, no vota por C y, cuando A vuelve, todas las réplicas reparan y conservan el valor.

### Licencia

Apache-2.0. Ver [LICENSE](LICENSE).

## EN

### What it is

A Raft-replicated KV store whose log lives on a simulated sector device that can silently corrupt sectors. Two policies share the same Raft core:

- **Vanilla**: when an entry fails its checksum, the node restarts and truncates the log from that entry, as it would for a crash-torn tail. This is the behaviour that loses committed data.
- **CTRL** (Alagappan et al., *Protocol-Aware Recovery for Consensus-Based Storage*, FAST'18):
  - Every entry has an identifier record (index, term, CRC) in a separate device region. The record tells a torn tail (no identifier, never acknowledged, dropped) from corruption (identifier present, kept).
  - A corrupted entry keeps its place in the log and still counts in elections, so a candidate missing it cannot win.
  - A follower gets it again from the leader.
  - The leader asks the others for it (`FixReq`/`FixResp`). It drops the entry only when a majority says it does not have it and its index is above the commit index; it then gives up its term.

The simulator injects sector corruption, crashes, torn tails, and message loss and reordering. Every run is judged by three checks:

- The Porcupine-style linearizability checker from [`cortex9-raft-sim`](https://github.com/federiconbaez/cortex9-raft-sim), used as a dependency pinned at `0b37dc1`.
- A state-machine-safety oracle (two replicas applying different entries at the same index).
- A count of truncations below the commit index.

### Measured results

Machine: Linux 6.12 VM, 8 vCPU Intel Xeon, rustc 1.99.1, release build. Numbers come from the binary built at commit `143a9a8`. Later commits only touch CI, docs and the round count of one test under Miri.

| Criterion | Command | Result | Status |
|---|---|---|---|
| 10k seeds, minority corruption, CTRL has no violations | `run --mode ctrl --faults minority --seeds 10000 --require-safe --min-recovery 0.99` | 0 violations, 0 undecided; 114,283 corrupted sectors injected, 113,210 repaired | PASS |
| Vanilla loses data under the same faults | `run --mode vanilla --faults minority --seeds 10000 --require-violation` | 1,232 of 10,000 seeds violate safety; 4,817,870 entries truncated by corruption | PASS |
| Automatic recovery ≥ 99% under minority corruption | same CTRL run | 10,000 of 10,000 seeds recovered (100.00%) | PASS |
| Fault-free overhead ≤ 10% | `overhead --seeds 1000 --reps 5 --max-overhead 0.10` | CPU +3.10% (median of 5); throughput 0.00%; latency 0.00%; disk bytes +6.25% | PASS |

Extra controls:

| Control | Command | Result |
|---|---|---|
| CTRL with every copy corrupted | `run --mode ctrl --faults majority --seeds 2000 --require-safe` | 0 violations; 0 of 2000 recovered (it stalls, as it should) |
| Negative control: drop after a single "I lack it" | `run --mode ctrl --faults majority --seeds 2000 --bug discard-on-one-lack --require-violation` | 53 of 2000 seeds violate safety: the checker catches it |
| 3 nodes, CTRL | `run --mode ctrl --nodes 3 --seeds 2000 --require-safe --min-recovery 0.99` | 0 violations; 2000 of 2000 recovered |
| 3 nodes, vanilla | `run --mode vanilla --nodes 3 --seeds 2000 --require-violation` | 1,119 of 2000 violate safety |

How each number is defined:

- **Minority corruption**: for every log index, at most ⌊(n−1)/2⌋ replicas ever get that index corrupted. Faults are a flipped bit or a zeroed sector, biased towards recent entries and sometimes followed by a crash.
- **Recovered**: three things hold at the end of the run. No replica holds a corrupted entry. Every replica has applied everything committed before the fault phase ended. Clients completed operations after the rolling restart, which forces each replica to re-read its log. The denominator is the seeds with at least one corruption injected.
- **Overhead**: corruption-free runs (still with crashes, torn tails and message loss) in both modes on the same seeds.
  - Without faults the protocol is identical, so completed operations and simulated latency match exactly.
  - The CPU difference is the cost of writing and verifying identifier records: wall time of the whole simulation, median of 5 interleaved repetitions.
  - In bytes, CTRL adds 32 bytes per 512-byte entry (6.25%).

### Limitations (honest)

- This is a deterministic simulation, not a real distributed system. There is no real disk and no `fsync`. Real-disk overhead (write ordering between a sector and its identifier) is **not measured**.
- Metadata (term and vote) is not corrupted in this model. CTRL stores it redundantly and it is assumed intact here.
- A single fault is assumed not to hit both an entry's sector and its identifier record. The paper makes the same assumption.
- No snapshots or membership changes.
- "Vanilla" models one concrete policy: truncate at the first bad entry. Other real systems do other things, such as crashing or ignoring the checksum.
- The 100% recovery relies on a 1500-tick fault-free healing phase. When a majority of copies of a committed entry is corrupted, CTRL does not recover: it stalls and does not lose data.
- CI has not run on GitHub yet: the repository only exists locally.

### Usage

See the commands in the Spanish section. `tests/scenarios.rs` walks through the core case step by step:

1. A write is committed on {A, B} while C is isolated.
2. B's copy is corrupted and A dies.
3. Under vanilla, B truncates, votes for C, and the write is gone.
4. Under CTRL, B keeps the identifier and refuses to vote for C. When A returns, every replica repairs and keeps the value.

### License

Apache-2.0. See [LICENSE](LICENSE).
