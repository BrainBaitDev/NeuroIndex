<div align="center">
  <img width="496" height="312" alt="logo_neuro_index" src="https://github.com/user-attachments/assets/875d11e0-aa42-4086-8f18-c298eeb0998e" />
</div>

<div align="center">

[🚀 Quick Start](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs/QUICK_START.md) • [📖 Docs](https://github.com/BrainBaitDev/NeuroIndex/blob/main/docs) 

</div>


 # <center>NEUROINDEX </center>
 
NeuroIndex è un motore di database in-memory di nuova generazione progettato per offrire prestazioni elevate, scalabilità orizzontale e robustezza lock-free.
Architettura multi-shard, supporto nativo al parallelismo su sistemi multi-core, e una serie di indici specializzati (hash table, alberi, filtri probabilistici) permettono di gestire efficientemente carichi intensivi di dati con query rapide e affidabili.

Il sistema combina tecniche avanzate di indicizzazione — Cuckoo Hashing, Adaptive Radix Tree (ART), Hopscotch Map e Cuckoo Filter — per rispondere sia a richieste puntuali, sia a ricerche su range, tag, valori e verifiche di esistenza.
La gestione della memoria è estremamente ottimizzata grazie all’utilizzo della Memory Arena, con blocchi pre-allocati e threshold per l’inserimento inline che riducono la frammentazione e accelerano accessi e operazioni batch.

NeuroIndex integra meccanismi di persistenza affidabili tramite Write-Ahead Log (WAL) e snapshot periodici, garantendo la resilienza ai crash e la rapidità nel recovery dei dati.

## TECNOLOGIE

NeuroIndex è sviluppato nativamente in __Rust__, scelto per le sue garanzie uniche di sicurezza della memoria e performance. 
L’architettura multi-shard lock-free sfrutta a fondo il modello di proprietà e concorrenza di Rust, consentendo accessi paralleli ai dati e garbage collection epoch-based senza rischi di race condition o corruzione della memoria.

Il modello di ownership di Rust e il suo sistema di tipi hanno reso la gestione della memoria basata su handle naturale, permettendo l’utilizzo efficiente di allocator ad arena e una pulizia automatica tramite il trait Drop. 
Il crate crossbeam-epoch viene impiegato per la garbage collection lock-free basata su epoch, rendendo possibili operazioni multi-threading e automatizzando la gestione del ciclo di vita della memoria — un aspetto chiave per database in-memory con milioni di entry concorrenti.

Il codice fa ampio uso di astrazioni zero-cost tramite tipi generici e traits, dando vita a motori flessibili (Engine<K, V>) e strutture d’indice modulari senza impatto sulle prestazioni a runtime. 
Il sistema dei trait consente separazioni chiare per serializzazione, hashing e ordinamenti custom, mentre gli adapter degli iterator favoriscono l’esecuzione di query su range in modo efficiente, senza allocazioni nell’heap.

Rust ha fornito un vantaggio significativo nella realizzazione di un database lock-free, sicuro nella concorrenza e persistente. 
Il risultato è un backend moderno con minore rischio di bug in memoria, meno crash in produzione e un core scalabile, pensato per applicazioni mission-critical e ambienti distribuiti always-on.

Nonostante un ritmo più lento nella fase di sperimentazione, la sicurezza e la manutenibilità offerte da Rust garantiscono benefici a lungo termine per infrastrutture database di nuova generazione.
 
## MOTIVAZIONE E OBIETTIVI

La crescente esigenza di gestire dati in tempo reale e in volumi sempre più elevati ha reso le soluzioni di database tradizionali spesso inadeguate per applicazioni moderne che richiedono bassissima latenza, alta scalabilità e robustezza operativa. 
I sistemi distribuiti e le architetture orientate ai microservizi necessitano non solo di performance elevate, ma anche di affidabilità in condizioni di concorrenza estrema, resilienza ai guasti e capacità di adattamento dinamico al carico.

NeuroIndex nasce con la motivazione di superare i classici limiti dei database general purpose offrendo:

- Un __database in-memory__ progettato per essere realmente lock-free e scalabile orizzontalmente.

- __Supporto nativo per query__ concorrenti, batch e operazioni su dati strutturati, taggati o sottoposti a frequenti aggiornamenti.

- Un sistema in grado di garantire la __persistenza__ anche in presenza di fault grazie a meccanismi robusti di snapshot e Write-Ahead Log.

- Un’architettura che consenta __ottimizzazioni avanzate sulle strutture dati__ sottostanti (hashing, tree, filtri probabilistici) per scenari specifici come search engine, caching distribuito, data analytics e sistemi real time.

L'obiettivo di NeuroIndex è fornire una piattaforma affidabile e moderna per sviluppatori e aziende che necessitano di gestire grandi quantità di dati in modo efficiente, con la sicurezza della concorrenza, la velocità delle operazioni in-memory e la tranquillità di una persistenza garantita e recovery immediato in caso di guasti.

## FEATURE CHIAVE
NeuroIndex si distingue per una serie di feature avanzate che rispondono alle esigenze dei sistemi moderni in termini di performance, integrabilità e scalabilità:

**Performance estrema**

- Architettura lock-free multi-shard con concorrenza nativa: operazioni su chiavi, valori, tag e range gestite in tempo costante (O(1)) o logaritmico (O(logN)).

- Pre-allocazione della memoria con Arena allocator e ottimizzazione inline per dati di piccole dimensioni.

- Pipeline di query parallela, ottimizzata per throughput elevato e latenza minima anche con carichi contemporanei importanti.

**Interfacce flessibili e moderne**

- API REST e gRPC per integrazioni rapide con microservizi, piattaforme cloud, sistemi di analytics ed e-commerce.

- Compatibilità nativa con protocollo RESP (Redis-like) per utilizzo come key-value store ultraveloce.

- SDK per Rust e Python, oltre alla possibilità di esportare metriche e logs verso sistemi di monitoring come Prometheus/Grafana.

**Scalabilità orizzontale**

- Gestione distribuita tramite multi-shard: possibilità di aumentare o ridurre il numero di shard in funzione delle risorse disponibili, sia in ambienti standalone che clusterizzati (Kubernetes, VM, bare-metal).

- Supporto alla replicazione dei dati e failover automatico per garantire alta disponibilità e continuità del servizio senza single point of failure.

- Elasticità nel bilanciamento del carico e nella gestione di dataset di grandi dimensioni.

**Persistenza e resilienza**

- Meccanismi robusti di Write-Ahead Log (WAL) e snapshot consistenti per il recupero rapido in caso di guasti.

- Garbage collection lock-free, validazione dell’integrità dati e recovery automatico per ridurre il rischio di perdite o corruzioni.

**Sicurezza e affidabilità**

- Il modello Rust riduce drasticamente bug come buffer overflow e use-after-free, abbassando l’incidenza di CVE e crash in produzione.

- Validazione automatica e isolamento degli errori a livello di shard.

## SCHEMA ARCHITETTURALE AD ALTO LIVELLO

L’architettura di NeuroIndex è organizzata in modo modulare e orientato alla scalabilità.
Al centro si trova il motore multi-shard, dove ciascun shard rappresenta una partizione indipendente e lock-free del datastore.
Le richieste in arrivo dai client (tramite API REST, gRPC, RESP o SDK) vengono gestite da un Query Router, che applica una funzione di hash sulla chiave per dirigere ogni operazione verso lo shard competente.

Ogni shard è strutturato con una serie di indici specializzati:

- **Cuckoo Hash Table** per le operazioni chiave-valore ultraveloci

- **Adaptive Radix Tree** (ART) per ricerche su range e ordinamenti

- **Hopscotch Map** per filtraggi su tag, valori e prefissi

- **Cuckoo Filter** per verifiche probabilistiche di esistenza

La memoria di ciascuno shard è gestita tramite arena allocator con garbage collection epoch-based lock-free (crossbeam-epoch).
Le operazioni di persistenza vengono garantite da un modulo dedicato che gestisce il Write-Ahead Log (WAL) e snapshot periodici dello stato del database.

***Flow di una richiesta tipica:***

1. Il client invia la richiesta tramite una delle interfacce disponibili.

2. Il Query Router determina lo shard responsabile.

3. Lo shard processa la query sfruttando gli indici appropriati (hash table, tree, maps, filtri).

4. Eventuali scritture vengono transazionalizzate su WAL e sincronizzate negli snapshot.

5. La risposta viene restituita al client.


![CLIENT  API -1-](assets/CLIENT%20%20API%20-1-.png)
Questa architettura garantisce alta efficienza, resilienza ai guasti e capacità di scalare dinamicamente in base alle esigenze dell’applicazione.

## PERFORMACE E METRICHE

Nel mondo dei database in-memory, la performance non è solo una questione di velocità, ma rappresenta anche la capacità del sistema di sostenere carichi intensivi, mantenere la reattività sotto stress e offrire metriche affidabili che consentano il tuning e il monitoraggio continuo.

I benchmark ufficiali di NeuroIndex sono stati concepiti per fornire una valutazione concreta e riproducibile delle prestazioni del motore, in modo da evidenziarne le capacità reali in scenari d’uso tipici quanto in condizioni estreme.
La metodologia impiegata si basa su test automatizzati e script specializzati che misurano il throughput, la latenza e la resilienza delle principali operazioni di database: inserimento (INSERT), recupero dati (GET), cancellazione (DELETE), scalabilità tramite sharding, velocità di recovery dopo crash e tempi di build del software.

Ogni test viene eseguito su hardware standardizzato, con configurazioni scalabili sia per numero di core sia per quantità di memoria RAM, al fine di studiare il comportamento del sistema in relazione alle risorse disponibili. Le operazioni sono monitorate attraverso contatori e metriche interne che captano i valori medi e di picco delle performance, mentre i risultati vengono validati con utility di profiling come PerfCounters e tramite verifica diretta delle risposte.

__La metodologia adottata include:__

- Misurazioni singole e batch su milioni di record, sia in condizioni di database “freddo” (appena avviato) che “caldo” (sottoposto a carichi prolungati).

- Test di throughput e latenza per ogni tipo di operazione, con particolare attenzione alla coerenza dei risultati nel tempo.

- Benchmark di recovery in cui si simula uno scenario di crash e si valuta “end-to-end” il tempo necessario per tornare online e rendere disponibili i dati.

- Verifica della scalabilità orizzontale tramite l’attivazione di più shard e monitoraggio dei picchi aggregati di operazioni.

- Analisi dei tempi di build e del processo di deployment, per evidenziare la rapidità nel ciclo di sviluppo e rilascio.

Questi benchmark non solo dimostrano il valore prestazionale di NeuroIndex, ma forniscono una base solida per il tuning e il confronto con soluzioni concorrenti, assicurando la trasparenza e la affidabilità delle metriche pubblicate.

__Risultati di Benchmark:__

- INSERT – 60 M ops/sec
Stabile anche su carichi intensivi, ideale per scenari di ingestione dati massiva (es. logging, IoT, eventi real-time).

- GET (SIMD) – 30.88 M ops/sec
Tempi di risposta ultra-rapidi su query puntuali, perfetto per applicazioni low-latency (cache, user session, fast analytics).

- DELETE – 15.04 M ops/sec
Elevata reattività anche in situazioni di rapido turnover dei dati, supporta use-case come TTL, purging o archivi temporanei.

- Sharding – 114.51 M ops/sec (16 shard)
Scalabilità quasi lineare: aggiungere shard (core) incrementa il throughput, rendendolo adatto a sistemi multi-core e clusterizzati.

- Recovery < 1s (100K keys)
Downtime trascurabile dopo crash o restart: garantisce alta disponibilità e ripristino veloce anche su dataset consistenti.

- Build time – 13.9s
Tempi di sviluppo, test e rilascio molto rapidi: consente pratiche DevOps avanzate e CI/CD fluida.

NeuroIndex offre performance eccellenti in tutte le principali operazioni, garantendo velocità, scalabilità e resilienza, elementi fondamentali per applicazioni moderne e mission-critical.
