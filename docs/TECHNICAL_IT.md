# INDICE
- [ARCHITETTURA INTERNA](#architettura-interna)
  - [1. Struttura di uno shard](#1-struttura-di-uno-shard)
  - [2. Gestione della memoria: Memory Arena](#2-gestione-della-memoria-memory-arena)
    - [Come funziona l'Arena?](#come-funziona-l'arena)
  - [3. Pipeline di Query](#3-pipeline-di-query)
- [GLI ALGORITMI](#gli-algoritmi)
  - [1. Cuckoo Hashing](#1-cuckoo-hashing)
    - [Resize Incrementale](#resize-incrementale)
  - [2. Adaptive Radix Tree (ART)](#2-adaptive-radix-tree-art)
  - [3. Hopscotch Hashing](#3-hopscotch-hashing)
  - [4. Cuckoo Filter](#4-cuckoo-filter)
  - [5. Logica di priorità tra indici](#5-logica-di-priorità-tra-indici)
- [GESTIONE RISORSE E EVICTION](#gestione-risorse-e-eviction)
  - [Volatile LRU: Eviction Intelligente](#volatile-lru-eviction-intelligente)
  - [Politiche di Eviction](#politiche-di-eviction)
- [CONSENSO DISTRIBUITO: RAFT](#consenso-distribuito-raft)
  - [Architettura Raft](#architettura-raft)
  - [Leader Election](#leader-election)
  - [Log Replication](#log-replication)
  - [Network Layer HTTP](#network-layer-http)
- [TRANSAZIONI ACID](#transazioni-acid)
  - [Two-Phase Locking (2PL)](#two-phase-locking-2pl)
  - [Snapshot Isolation](#snapshot-isolation)
  - [Commit e Rollback](#commit-e-rollback)
- [AUTO-TUNING E OTTIMIZZAZIONE](#auto-tuning-e-ottimizzazione)
  - [Sistema di Auto-Tuning](#sistema-di-auto-tuning)
  - [Metriche e Raccomandazioni](#metriche-e-raccomandazioni)
- [PERSISTENZA E FAILOVER](#persistenza-e-failover)
  - [WAL (Write-Ahead Log) e snapshot](#wal-write-ahead-log-e-snapshot)
  - [Recovery e resistenza ai crash](#recovery-e-resistenza-ai-crash)


<div style="page-break-after: always;"></div>

# ARCHITETTURA INTERNA

NeuroIndex adotta una architettura **multi-shard lock-free** che sfrutta al massimo la concorrenza su sistemi multi-core, garantendo altissime prestazioni e scalabilità orizzontale. 
Ogni shard rappresenta un’istanza indipendente che gestisce una porzione del dataset tramite una combinazione di algoritmi di indicizzazione ottimizzati per specifici carichi: Cuckoo Hashing per accessi chiave-valore rapidi, Adaptive Radix Tree per ricerche ordinate e range, Hopscotch Map per gestione di tag, valori e prefissi, e Cuckoo Filter per verifiche probabilistiche dell’esistenza. 
A livello di memoria, ogni shard allocca i valori in una arena dedicata, minimizzando la frammentazione.

Questa stratificazione di indici specializzati permette a NeuroIndex di rispondere efficientemente a un’ampia gamma di query (lookup, aggregazioni, ricerche per prefisso e tag) mantenendo la concorrenza senza lock, e supportando operazioni di persistenza (WAL, snapshot) e recovery robusta. 
L’intera architettura è progettata per adattarsi dinamicamente al carico e scalare sia in ambienti standalone che cluster distribuiti

![CLIENT  API](CLIENT%20%20API.png)

## 1. Struttura di uno shard
Uno Shard rappresenta una partizione indipendente del datastore, progettata per lavorare in parallelo con altri shard, sfruttando la concorrenza multi-core e incrementando la scalabilità e l’efficienza del database.

***Componenti Principali di uno Shard***
1. __Memory Arena__: Gestisce l’allocazione della memoria dei dati (i valori V), offrendo performance costanti per allocazione e deallocazione e minimizzando la frammentazione.
   Utile nei carichi intensivi dove frequenti scritture e cancellazioni richiedono efficienza nella gestione della RAM.

2. __Tabella Hash Principale (CuckooTable)__: È la struttura centrale per la memorizzazione delle coppie chiave-valore.
   Utilizza il Cuckoo Hashing per lookup, insert e delete costanti (O(1)), anche con carichi ad alta densità di dati e garantisce velocità e collisioni bassissime.

3. __Indice Ordinato (ArtTree)__: Un Adaptive Radix Tree (ART), usato per query su range di chiavi, iterazioni ordinate e ricerche per prefisso.
   Ottimizzato per keys anche di grandi dimensioni (stringhe, UUID), offre prestazioni eccellenti su ricerche “composte” o batch.

4. __Indice Tag (HopscotchMap)__: Implementazione di Hopscotch Hashing focalizzata sulla ricerca e il filtro per tag o metadati associati ai record. 
   Permette query come “dammi tutti i record con tag X”.

5. __Indice Reverse Tag (HopscotchMap)__: Permette la ricerca inversa: dal tag ai record che lo includono. Fondamentale per implementare relazioni molti-a-molti tra tag e dati.

6. __Indice per Valore (HopscotchMap)__: Facilita la ricerca/filtraggio per valori (non solo per chiave). Utile in analytics o motori di regole.

7. __Indice Prefisso (HopscotchMap)__: Gestito per ottimizzare ricerche per prefisso delle chiavi senza dover scansionare interamente le chiavi (es. “tutti i record che iniziano per ‘user:’”).

8. __Filtro di Esistenza CuckooFilter)__: Un filtro probabilistico che permette di capire rapidamente se una chiave non esiste, riducendo il numero di lookup costosi.
   
Struttura di uno shard in Rust
```rust
pub struct Shard<K, V> {
    arena: Arc<Arena<V>>,
    kv: CuckooTable<K, Handle<V>, RandomState>,
    ordered: ArtTree<K, Handle<V>>,
    tags: HopscotchMap<u64, Vec<Handle<V>>>,
    reverse_tags: HopscotchMap<usize, Vec<u64>>,
    value_index: HopscotchMap<String, Vec<K>>,
    prefix_index: HopscotchMap<String, Vec<K>>,
    bloom: Mutex<CuckooFilter>
}
 
}

```
<br>

***VANTAGGI DI QUESTO TIPO DI ARCHITETTURA***
1. __Parallellismo estremo__: Ogni shard lavora in autonomia, massimizzando l’uso dei core CPU.

2. __Contenimento degli errori__: Crash o degradi su uno shard non impattano l’intero datastore.

3. __Scalabilità__ : Facile aumentare o ridurre il numero di shard a seconda delle risorse hardware.

4. __Efficienza delle query__: Ogni indice fornisce il percorso più rapido per il tipo di query richiesto, riducendo la latenza globale.


## 2. Gestione della memoria: Memory Arena

NeuroIndex adotta la gestione della memoria tramite __ Memory Arena__ per ottenere eccellenti prestazioni e semplificare la complessità di allocazione che caratterizza i database in-memory. 

***Parametri tecnici della Memory Arena:***

- __BLOCK_SIZE = 1024 slot__: La memoria viene allocata in blocchi da 1024 slot ciascuno, ottimizzando la gestione degli oggetti e riducendo l’overhead sia nelle operazioni di scrittura che di cancellazione.

- __INLINE_THRESHOLD = 32 bytes__: Ogni valore con dimensione inferiore o uguale a 32 byte viene storicizzato in modalità inline, evitando allocazioni separate e migliorando la velocità di accesso.

Grazie all’Arena, i dati vengono allocati in grandi blocchi, sui quali il sistema distribuisce i singoli oggetti delle operazioni di scrittura e modifica. 
Questo approccio consente di svolgere le operazioni di allocazione e deallocazione in maniera più rapida e prevedibile rispetto ai tradizionali metodi come la malloc o il garbage collector, riducendo drasticamente la frammentazione della memoria — problema tipico quando si gestiscono milioni di record in RAM.

L’Arena semplifica inoltre la gestione del ciclo di vita dei dati: l’eliminazione di tutti i record associati a uno shard si risolve con la distruzione dell’intera arena, senza dover liberare manualmente ogni spazio occupato. Sul piano architetturale, questa soluzione si rivela ideale per modelli lock-free e per strategie batch, perché permette a thread multipli di operare sugli stessi dati senza i contenziosi tipici dei lock.

In fase di snapshot o recovery, l’arena rende più immediata la serializzazione e la ricostruzione dei dati, consentendo di copiare o scansionare rapidamente l’insieme dei valori allocati. 
Tutti gli indici delle strutture – hash table, tree, tag – puntano agli handle della memoria arena, garantendo coerenza tra le diverse viste del database e riducendo il rischio di problemi come memory leak, doppie allocazioni o condizioni di race sulla memoria.

La scelta dell’arena in NeuroIndex non è solo una questione di performance, ma rappresenta una strategia ingegneristica che favorisce la robustezza, la scalabilità, la semplicità di gestione e la sicurezza del sistema nei carichi reali.

### Come funziona l’Arena?
- L'Arena pre-alloca un grande blocco di memoria (block allocation) e gestisce tutti i dati (V) indirizzando e spostando solo i riferimenti (handle).

- Quando serve memorizzare un nuovo valore, alloca lo spazio richiesto all’interno dell’arena invece di fare una malloc o new per ogni inserimento.

- La deallocazione è gestita in modo efficiente, spesso tramite la liberazione di interi blocchi, riducendo l’overhead tipico del garbage collection o dei free frammentati.

## 3. Pipeline di Query

La pipeline di query in NeuroIndex rappresenta il flusso strutturato e ottimizzato con cui una richiesta, proveniente da un client tramite una delle interfacce supportate (RESP, HTTP, gRPC, API Rust), viene elaborata all’interno del sistema e restituisce una risposta finale. 
Questo percorso non è solo una sequenza di operazioni, ma incarna la filosofia lock-free e multi-shard che rende NeuroIndex altamente performante.

Tutto inizia con la ricezione della richiesta da parte del server, che può provenire da diversi protocolli ma che viene uniformata in una rappresentazione interna comune. 
A questo punto, il Query Router svolge un ruolo fondamentale: utilizza una funzione di hash sulla chiave della richiesta per determinare a quale shard del database inoltrarla. 
Questo routing garantisce che ogni shard riceva solo le richieste relative ai dati di cui è responsabile, eliminando la necessità di lock globali e consentendo un’efficienza massima nell’accesso concorrente.

All’interno dello shard, la Query attraversa una serie di indici specializzati; il sistema verifica prima tramite filtri probabilistici (Cuckoo Filter) se la chiave è potenzialmente presente, velocizzando i fallimenti. 
In caso positivo, si passa al lookup nella cuckoo hash table per le ricerche puntuali, se il tipo di ricerca prevede range, ordinamenti o prefissi, la query viene indirizzata alla struttura appropriata (tipicamente l’Adaptive Radix Tree); mentre per ricerche filtrate su metadati, tag o valori, vengono utilizzate le hash map Hopscotch. 
Ogni indice è ottimizzato per rispondere velocemente a una specifica tipologia di accesso, riducendo al minimo la latenza generale.

Oltre al percorso di lettura, la pipeline gestisce in modo atomico e sicuro anche le operazioni di scrittura, aggiornamento e cancellazione, con persistenza garantita tramite Write-Ahead Log e snapshot coordinati, così che la durabilità dei dati non venga mai compromessa.

Infine, il risultato viene convertito nel formato richiesto dal protocollo di ingresso e restituito al client. 
Questo design a pipeline, orchestrato e parallelo, massimizza l’uso delle risorse hardware, aumenta la scalabilità e riduce drasticamente i tempi di risposta, permettendo a NeuroIndex di sostenere carichi intensivi e contemporanei senza degrado delle prestazioni.

<div style="page-break-after: always;"></div>


# GLI ALGORITMI
Per garantire prestazioni di alto livello, scalabilità e flessibilità nelle query, NeuroIndex adotta una combinazione articolata di algoritmi d’indicizzazione, ciascuno specializzato nella gestione di particolari tipologie di interrogazione o pattern d’accesso ai dati. Queste strutture lavorano in modo sinergico all’interno di ogni shard, permettendo al sistema di rispondere efficacemente tanto a lookup puntuali quanto a ricerche su range, filtro per tag, analisi su valori o controlli d’esistenza. Comprendere il ruolo e il funzionamento di ciascun algoritmo consente di apprezzare le scelte architetturali che rendono NeuroIndex un motore di database in-memory moderno e altamente performante.
Nella sezione seguente, vengono illustrate in modo dettagliato le principali strutture utilizzate: Cuckoo Hashing, Adaptive Radix Tree, Hopscotch Hashing e i filtri probabilistici, con i relativi vantaggi d’implementazione.

## 1. Cuckoo Hashing
Il Cuckoo Hashing è una tecnica avanzata di gestione delle tabelle hash, progettata per offrire operazioni di ricerca, inserimento e cancellazione estremamente rapide e affidabili, anche in presenza di carichi di dati elevati. 
Il suo nome deriva dal comportamento del cuculo, che depone le uova nei nidi altrui: analogamente, in una tabella hash cuckoo, ogni elemento può potenzialmente “sfrattare” un elemento già presente e prenderne il posto qualora lo spazio diretto sia occupato.

Il funzionamento si basa su più funzioni di hash indipendenti: quando si inserisce una chiave, la si può collocare in uno tra diversi possibili slot calcolati dalle funzioni di hash. 
Se lo slot prescelto è già occupato, è possibile "rimbalzare" l’elemento esistente in un’altra posizione, seguendo le sue stesse funzioni hash. 
Questo processo di ricollocamento, detto cuckooing, prosegue finché si trova un posto libero oppure si decide di ridimensionare la tabella se la catena di spostamenti diventa troppo lunga.

Grazie a questa architettura, il Cuckoo Hashing garantisce tempi di accesso deterministici e rapidi: ogni operazione di lookup consiste in uno o pochi controlli diretti sugli slot associati alla chiave, senza bisogno di scandire catene di collisioni come nelle hash tradizionali. 
Questo riduce fortemente la possibilità di degradazione delle performance tipica dei casi di sovraccarico.

I principali vantaggi di questa soluzione sono la prevedibilità — il tempo di accesso non dipende dal livello di riempimento della tabella o dalla frequenza di collisioni — e la robustezza, in quanto la struttura resiste bene anche a carichi molto elevati senza impatti significativi sulla velocità. 
Inoltre, il cuckoo hashing si adatta perfettamente a implementazioni lock-free e multi-thread, perché ogni posizione può essere aggiornata rapidamente senza coinvolgere l’intera struttura.

L’adozione del Cuckoo Hashing come motore primario per la gestione delle chiavi garantisce lookup rapidissimi e affidabili a qualsiasi scala operativo, contribuendo in modo sostanziale alla capacità del sistema di gestire query in tempo reale anche quando il volume dei dati cresce in modo significativo.

### Resize Incrementale

Una delle innovazioni chiave di NeuroIndex è l'implementazione del **resize incrementale** per la CuckooTable, che risolve uno dei problemi storici delle hash table: il blocco durante il ridimensionamento.

Tradizionalmente, quando una hash table raggiunge il suo limite di capacità, deve essere ridimensionata in un'unica operazione atomica che blocca tutte le altre operazioni. In NeuroIndex, il resize avviene in modo incrementale:

- **Allocazione Lazy**: La nuova tabella viene allocata ma il trasferimento dei dati avviene gradualmente
- **Doppia Ricerca**: Durante il resize, le ricerche controllano sia la vecchia che la nuova tabella
- **Trasferimento Batch**: I dati vengono spostati in piccoli batch durante le normali operazioni
- **Gestione Overflow Stash**: Se lo stash (area di overflow) si riempie durante il resize, il sistema completa automaticamente il ridimensionamento con un retry loop robusto

Questo approccio garantisce che:
1. Le operazioni di lettura/scrittura non vengano mai bloccate
2. Il throughput rimanga costante anche durante il resize
3. La memoria venga utilizzata in modo efficiente
4. Non ci siano perdite di dati anche in caso di overflow

Il meccanismo di `finish_resize` assicura che, anche in condizioni di carico estremo, il resize venga completato correttamente senza compromettere l'integrità dei dati.

## 2. Adaptive Radix Tree (ART)
L'Adaptive Radix Tree (ART) fornisce un indice ordinato ottimizzato per range query e ricerche per prefisso. L'implementazione attuale utilizza un BTreeMap di Rust altamente ottimizzato che garantisce operazioni O(log n) con eccellente località di cache. È disponibile un'implementazione ART completa con nodi adattivi (Node4/16/48/256) e ricerca SIMD-accelerated per dataset superiori ai 10M di chiavi.
La vera innovazione dell’ART sta nel suo carattere adattivo: anziché mantenere una struttura fissa per ogni nodo, ART regola dinamicamente il tipo di nodo usato, in base alla densità delle chiavi che ricadono in quel segmento, ottimizzando così sia lo spazio sia la velocità di navigazione.

Questa adattività consente all’ART di essere straordinariamente rapido nelle operazioni di ricerca, inserimento e cancellazione, ma ancor più vantaggioso quando si tratta di range query e ordinamento. 
Le range query (ad esempio, trovare tutte le chiavi tra "chiave:0001000" e "chiave:0005000") sono estremamente efficienti perché l’albero rappresenta le chiavi in modo ordinato: una ricerca può identificare rapidamente il punto di partenza e proseguire, attraversando solo i nodi dell’intervallo desiderato, senza dover scandire l’intera struttura. 
Analogamente, questa rappresentazione ordinata permette di iterare facilmente le chiavi in ordine crescente o decrescente – operazione utile per ordinamenti, paginazione, e aggregazioni.

L’ART, inoltre, gestisce molto bene anche la ricerca per prefisso, ad esempio: trovare “tutte le chiavi che cominciano con ‘chiave:’”. 
La struttura consente di individuare rapidamente il ramo dell’albero che rappresenta quel prefisso e poi scorrere solo i nodi correlati, riducendo enormemente il numero di confronti rispetto a una lista o a una hash table.

L’ART si affianca agli altri indici fornendo la capacità di rispondere in modo eccellente alle query che richiedono range, ordinamento e prefisso, andando oltre le classiche ricerche puntuali per chiave. 
Grazie a questo NeuroIndex offre funzionalità avanzate come paginazioni ordinate, analytics, aggregazioni e query batch estremamente veloci e scalabili, senza sacrificare la velocità sulle operazioni di inserimento, cancellazione e aggiornamento.

## 3. Hopscotch Hashing
Hopscotch Hashing è un'evoluzione brillante delle tecniche di hashing tradizionali in cui invece di cercare di evitare del tutto le collisioni, come avviene nel cuckoo hashing, questo metodo gestisce le collisioni mantenendo i dati “vicini” alle posizioni preferite calcolate dalla funzione hash.
La chiave del sistema sta nella capacità, appunto, di “saltellare” su una finestra limitata di bucket adiacenti: se lo slot principale è occupato, l’algoritmo cerca rapidamente nelle posizioni vicine in cui inserire il nuovo elemento, garantendo così che ogni record possa essere trovato in un numero contenuto e prevedibile di passaggi.

Questa prossimità geografica dei dati nella struttura facilita non solo le operazioni di ricerca, inserimento e cancellazione, ma si rivela strategica per le applicazioni che richiedono la gestione efficiente di tag, valori e prefissi. 
Quando NeuroIndex deve indicizzare o cercare dati in base a un tag (ad esempio, tutte le chiavi con il tag “active”), il sistema utilizza una hopscotch map per tenere traccia delle associazioni tra tag e record: ogni ricerca sarà rapida e prevedibile, grazie alla localizzazione dei dati. 
Lo stesso approccio si applica alla gestione di indici secondari per valori, permettendo di filtrare velocemente tutti i record che hanno un determinato valore associato, e per le ricerche per prefisso, dove la distribuzione localizzata degli bucket riduce sensibilmente la latenza rispetto a strutture meno specializzate.

L’implementazione Hopscotch si adatta molto bene ai carichi elevati e alle operazioni concorrenti, proprio perché riduce il rischio di catene lunghe di collisioni e rende la struttura agile sia su accessi random che sequenziali. Un altro vantaggio importante è nella gestione delle cancellazioni e della modifica dinamica di tag, valori o prefissi: le mappe hopscotch consentono di mantenere sempre sotto controllo il livello di riempimento e la distribuzione dei dati, favorendo la performance anche quando il dataset è molto grande o soggetto a continue variazioni.

In NeuroIndex, il ricorso all’Hopscotch Hashing per indici di tag, valore e prefisso si traduce in una flessibilità d’uso elevata e in un supporto ottimale ad operazioni di analisi, filtro e ricerca, essenziali per sistemi che devono rispondere in tempo reale a query complesse e simultanee. Questo contribuisce a rendere il database capace di gestire scenari dal caching fino all’analisi dati avanzata senza perdere in rapidità né affidabilità strutturale.

## 4. Cuckoo Filter
I filtri probabilistici come il Cuckoo Filter sono strumenti fondamentali per accelerare alcune operazioni chiave all’interno di database moderni come NeuroIndex, in particolare quando occorre verificare rapidamente l’esistenza (o la possibile esistenza) di una chiave prima di effettuare operazioni più costose sulle strutture dati principali.

Il Cuckoo Filter oltre a mantenere uno spazio di memoria ristretto e a garantire bassa probabilità di falsi positivi, permette anche la cancellazione e l’aggiornamento degli elementi. 
Il Cuckoo Filter applica la logica del cuckoo hashing, gestendo inserimenti e spostamenti degli “impronte” in modo dinamico, mantenendo la struttura sempre compatta e pronta a rispondere velocemente alle query di verifica.

Nel concreto, ogni volta che in NeuroIndex arriva una richiesta per una chiave, il sistema può sfruttare questi filtri: se la risposta indica che la chiave sicuramente non c’è, si evita di andare a interrogare le strutture primarie, risparmiando prezioso tempo (soprattutto su dataset molto grandi). Solo in caso di esito incerto (potrebbe esistere) la ricerca prosegue sulle strutture principali.

Questa strategia permette di abbattere la latenza delle query negative e ridurre notevolmente il carico sulle zone più costose della pipeline, garantendo che il database resti veloce e reattivo anche quando il numero di chiavi gestite è nell’ordine dei milioni o più. 

## 5. Logica di priorità tra indici
La logica di priorità tra indici in NeuroIndex è il principio che regola l’ordine e la sequenza con cui vengono interrogate le varie strutture dati per rispondere a una query. 
La scelta del percorso di ricerca ottimale non solo migliora la velocità di risposta, ma riduce il carico sulle risorse e garantisce che ogni tipo di query riceva la risposta più corretta nel minor tempo possibile.

Quando una richiesta raggiunge lo shard designato, NeuroIndex non interroga tutti gli indici indiscriminatamente: punta invece a un approccio guidato, in cui il tipo di query determina quale struttura viene usata per prima. 
Per esempio, se si tratta di una lookup puntuale (chiave esatta), la ricerca parte dal filtro probabilistico (Cuckoo Filter): se la chiave non è presente, la risposta può essere fornita immediatamente, senza nemmeno accedere alle strutture principali. 
In caso di esito positivo, la pipeline prosegue con la tabella hash cuckoo, dove il retrieval è sempre O(1) e quindi estremamente veloce.
Se la query richiede un intervallo di chiavi, ordinamento o ricerca per prefisso, NeuroIndex indirizza la richiesta all’Adaptive Radix Tree: qui la gerarchia ordinata e la rappresentazione digitale delle chiavi consentono di individuare rapidamente il range desiderato, iterando solo sui nodi rilevanti. 
Analogamente, per richieste su tag, metadati o valori (ad esempio “tutti gli oggetti con tag active”), la pipeline sfrutta gli indici Hopscotch, ottimizzati per queste associazioni e capaci di filtrare grandi volumi in tempi molto contenuti.

Questa logica di priorità riduce la latenza generale perché evita di eseguire operazioni costose dove non necessario e garantisce che ogni query trovi la risposta percorrendo il cammino più breve. 
In più, la suddivisione degli indici permette di gestire accessi concorrenti senza lock, favorendo la scalabilità e la robustezza dell’intero sistema.

In sintesi, la logica di priorità tra indici in NeuroIndex è un mosaico coordinato di strategie data-driven, dove il sistema sa esattamente quale indice usare per ogni tipo di richiesta, ottenendo performance ottimali e massima efficienza operativa a qualsiasi scala di carico.

<div style="page-break-after: always;"></div>

# PERSISTENZA E FAILOVER
In un sistema di database in-memory progettato per performance estreme come NeuroIndex, la persistenza dei dati e la capacità di recuperare rapidamente da guasti rappresentano aspetti fondamentali per garantire affidabilità, durabilità e continuità del servizio. 
La sezione che segue approfondisce le strategie adottate per la conservazione sicura dello stato, come il Write-Ahead Log (WAL) e i meccanismi di snapshot, oltre alle soluzioni implementate per il failover automatico e la resilienza contro fault hardware o software. Queste componenti rendono NeuroIndex non solo veloce, ma anche un pilastro affidabile su cui costruire applicazioni critiche e always-on.

## WAL (Write-Ahead Log) e snapshot 
Il Write-Ahead Log (WAL) e gli snapshot rappresentano i due pilastri fondamentali della persistenza dei dati in NeuroIndex. 
Il WAL è un registro lineare di tutte le operazioni di modifica che vengono eseguite sul database: ogni inserimento, aggiornamento o cancellazione viene prima scritto in maniera transazionale sul log, e solo successivamente applicato alle strutture dati in-memory. 
Questo meccanismo assicura che, in caso di interruzione improvvisa del sistema (crash, spegnimento, errore hardware), sia sempre possibile ricostruire lo stato esatto del database semplicemente "ri-giocando" le voci del WAL dalla posizione più recente.

Parallelamente al WAL, NeuroIndex esegue periodicamente degli snapshot, ovvero una copia puntuale e consistente dell’intero dataset in-memory ad un dato istante. 
Lo snapshot permette di memorizzare lo stato globale del database senza dover scorrere tutte le voci singole del log: all’avvio, il sistema può così ripristinare rapidamente la versione più recente del dataset e, se necessario, integrare le modifiche successive richiamando solo le voci del WAL prodotte dopo quello snapshot.

Il binomio WAL+Snapshot ottimizza sia la durabilità dei dati (nessuna operazione è persa tra un crash e la successiva riapertura), sia la velocità di recovery: il ripristino non richiede la ri-esecuzione dell’intero log, ma solo delle modifiche più recenti. In pratica, il WAL garantisce che ogni transazione sia committata su disco prima di essere eseguita in memoria, mentre lo snapshot riduce i tempi di bootstrap e minimizza il rischio di corruzione in caso di errori. 
Inoltre, la generazione degli snapshot viene gestita in modo non bloccante, il sistema continua a rispondere alle query e alle operazioni in scrittura durante la copia.

In sintesi, WAL e snapshot sono strumenti complementari che assicurano alla persistenza di NeuroIndex massima affidabilità, coerenza e rapidità: le operazioni giornaliere sono sempre protette, e l’avvio o il recovery dopo un guasto sono efficienti e sicuri.


## Recovery e resistenza ai crash

La capacità di NeuroIndex di garantire recovery e resistenza ai crash è uno degli aspetti più critici per l’affidabilità di qualsiasi motore di database, soprattutto quando impiegato in contesti enterprise e applicazioni che richiedono alta disponibilità.

Il recovery inizia dalla sinergia tra il Write-Ahead Log (WAL) e gli snapshot: quando il sistema subisce un crash, sia esso causato da errori software, spegnimenti imprevisti o guasti hardware, NeuroIndex non perde mai la traccia delle operazioni fondamentali. Al riavvio, il database effettua prima il ripristino dello stato di memoria a partire dall’ultimo snapshot consistente, così da non dover procedere con una ricostruzione completa, che sarebbe onerosa in termini di tempo e risorse. Successivamente, vengono processate tutte le operazioni contenute nel WAL registrate dopo lo snapshot, garantendo che lo stato finale corrisponda esattamente a quello che c’era al momento del crash.

Questa procedura protegge sia i dati — che restano sempre coerenti e non subiscono corruzioni — sia la velocità di ripartenza del sistema, fondamentale per rientrare online in tempi brevissimi. NeuroIndex aggiunge inoltre meccanismi di controllo e verifica dell’integrità delle strutture di memoria durante il recovery: ogni handle e record viene validato affinché non vi siano incongruenze, leak o correlazioni errate tra le strutture.

Dal punto di vista della resistenza, NeuroIndex è progettato per minimizzare i punti di failure: la distribuzione lock-free tra shard e la separazione tra indici e memoria riducono le probabilità che un errore locale possa propagarsi e causare danni estesi. Anche in presenza di crash parziali, il sistema può riavviare solo i componenti interessati, senza compromettere l’intero database.

In sintesi, la strategia di recovery e resistenza ai crash di NeuroIndex garantisce che ogni dato sia sempre protetto, recuperabile e integrale, permettendo al database di ripristinare rapidamente il servizio senza perdere informazioni né degradare la performance, anche nelle situazioni più critiche.

<div style="page-break-after: always;"></div>

# GESTIONE RISORSE E EVICTION

La gestione efficiente della memoria è cruciale per un database in-memory come NeuroIndex. Il sistema implementa politiche di eviction sofisticate che bilanciano performance e utilizzo delle risorse.

## Volatile LRU: Eviction Intelligente

La **Volatile LRU** (Least Recently Used) è un'innovazione chiave di NeuroIndex che risolve un problema comune nei sistemi di caching: come evitare di rimuovere dati persistenti quando la memoria è piena.

### Principio di Funzionamento

A differenza delle politiche LRU tradizionali che evictano qualsiasi chiave meno recentemente usata, la Volatile LRU opera selettivamente:

- **Solo chiavi con TTL**: Vengono considerate per l'eviction solo le chiavi che hanno un Time-To-Live impostato
- **Protezione dati persistenti**: Le chiavi senza TTL (dati persistenti) non vengono mai rimosse automaticamente
- **Priorità basata su accesso**: Tra le chiavi volatili, vengono rimosse prima quelle meno recentemente accedute

### Vantaggi

1. **Separazione Logica**: Dati di sessione/cache (volatili) vs dati applicativi (persistenti)
2. **Prevedibilità**: Gli sviluppatori sanno che i dati persistenti non verranno mai evicted
3. **Efficienza**: Riduce il churn della cache evitando di rimuovere dati che verranno richiesti nuovamente
4. **Flessibilità**: Permette di usare lo stesso database per caching e storage persistente

### Implementazione

```rust
// Esempio di utilizzo
engine.put_with_ttl("session:123", data, Duration::from_secs(3600)); // Volatile
engine.put("user:456", user_data); // Persistente, mai evicted

// Quando la memoria è piena, solo "session:123" può essere rimosso
```

## Politiche di Eviction

NeuroIndex supporta multiple politiche di eviction configurabili:

### 1. LRU (Least Recently Used)
- Rimuove le chiavi meno recentemente accedute
- Ideale per workload con località temporale

### 2. LFU (Least Frequently Used)
- Rimuove le chiavi meno frequentemente accedute
- Ottimale per workload con pattern di accesso stabili

### 3. Volatile LRU
- Rimuove solo chiavi con TTL, in ordine LRU
- Perfetto per scenari misti cache+storage

### 4. Volatile LFU
- Rimuove solo chiavi con TTL, in ordine LFU
- Combina i vantaggi di LFU e protezione dati persistenti

### Resource Limits

Il sistema permette di configurare limiti precisi:

```rust
let limits = ResourceLimits {
    max_memory: Some(1024 * 1024 * 1024), // 1GB
    max_keys: Some(1_000_000),             // 1M chiavi
    eviction_target_percentage: 0.8,       // Evict fino all'80%
};

let engine = Engine::with_shards(4, 16)
    .with_resource_limits(limits)
    .with_eviction_policy(EvictionPolicy::VolatileLRU);
```

Quando i limiti vengono raggiunti, il sistema:
1. Identifica le chiavi candidate per l'eviction secondo la politica scelta
2. Rimuove le chiavi fino a raggiungere l'`eviction_target_percentage`
3. Continua a servire richieste senza interruzioni

<div style="page-break-after: always;"></div>

# CONSENSO DISTRIBUITO: RAFT

NeuroIndex implementa il protocollo **Raft** per garantire consenso distribuito e replicazione dei dati across multiple nodi, trasformando il database da sistema standalone a soluzione distribuita fault-tolerant.

## Architettura Raft

L'implementazione Raft in NeuroIndex segue fedelmente il paper originale "In Search of an Understandable Consensus Algorithm" di Ongaro e Ousterhout, con ottimizzazioni specifiche per database in-memory.

### Componenti Principali

1. **RaftNode**: Rappresenta un singolo nodo nel cluster
   - Mantiene lo stato (Follower, Candidate, Leader)
   - Gestisce il log delle operazioni
   - Coordina elezioni e replicazione

2. **Log Replication**: Ogni operazione di scrittura viene:
   - Proposta dal leader
   - Replicata sui follower
   - Committata quando la maggioranza conferma

3. **State Machine**: Background thread che applica le entry committate allo storage

### Stati del Nodo

- **Follower**: Stato iniziale, riceve append_entries dal leader
- **Candidate**: Richiede voti per diventare leader
- **Leader**: Coordina le operazioni di scrittura e invia heartbeat

## Leader Election

Il meccanismo di elezione garantisce che ci sia sempre un solo leader nel cluster:

1. **Timeout Elettorale**: Se un follower non riceve heartbeat, diventa candidate
2. **Request Vote**: Il candidate richiede voti agli altri nodi
3. **Maggioranza**: Serve la maggioranza dei voti per diventare leader
4. **Term**: Ogni elezione incrementa il term, prevenendo elezioni multiple

### Log Consistency

Prima di concedere il voto, ogni nodo verifica che il candidate abbia un log almeno aggiornato quanto il proprio, garantendo che il nuovo leader abbia tutte le operazioni committate.

## Log Replication

Quando il leader riceve una richiesta di scrittura:

```rust
// Client propone operazione
engine.put("key", "value"); 

// Leader:
// 1. Aggiunge al proprio log
// 2. Invia append_entries ai follower
// 3. Attende conferma dalla maggioranza
// 4. Commita l'operazione
// 5. Applica alla state machine
```

### AppendEntries RPC

Il leader invia periodicamente `append_entries` per:
- **Heartbeat**: Mantenere la leadership (ogni 50ms)
- **Replicazione**: Propagare nuove entry del log
- **Commit**: Notificare il commit_index ai follower

### State Machine Application

Un background thread monitora continuamente:
```rust
loop {
    let committed_entries = raft.get_committed_entries();
    for entry in committed_entries {
        apply_to_storage(entry); // Put/Delete sulla CuckooTable
        raft.update_last_applied(entry.index);
    }
    sleep(10ms);
}
```

## Network Layer HTTP

NeuroIndex implementa un network layer basato su HTTP per la comunicazione tra nodi Raft:

### RPC Protocol

Due tipi di RPC principali:

1. **RequestVote**: Per le elezioni
```json
{
  "term": 5,
  "candidate_id": "node-1",
  "last_log_index": 100,
  "last_log_term": 4
}
```

2. **AppendEntries**: Per replicazione e heartbeat
```json
{
  "term": 5,
  "leader_id": "node-1",
  "prev_log_index": 99,
  "prev_log_term": 4,
  "entries": [...],
  "leader_commit": 98
}
```

### Implementazione

- **Client HTTP**: `ureq` per inviare richieste
- **Server HTTP**: `tiny_http` per ricevere richieste
- **Serializzazione**: JSON via `serde_json`
- **Trasporto**: HTTP POST su endpoint dedicati

### Configurazione

```rust
let raft = RaftNode::new("node-1")
    .with_network("127.0.0.1:5001");

let engine = Engine::with_shards(4, 16)
    .with_raft(raft);

// Avvia applier thread
Engine::start_raft_applier(&engine);
```

### Vantaggi

- **Fault Tolerance**: Sopravvive a crash di nodi minority
- **Consistency**: Garantisce linearizability delle scritture
- **Availability**: Continua a funzionare con majority dei nodi
- **Partition Tolerance**: Gestisce network partitions correttamente

<div style="page-break-after: always;"></div>

# TRANSAZIONI ACID

NeuroIndex implementa transazioni ACID complete, permettendo operazioni multi-key atomiche con garanzie di consistenza e isolamento.

## Two-Phase Locking (2PL)

Il sistema utilizza **Two-Phase Locking** per garantire la serializzabilità delle transazioni:

### Fasi del Locking

1. **Growing Phase**: La transazione acquisisce lock ma non ne rilascia
2. **Shrinking Phase**: La transazione rilascia lock ma non ne acquisisce

### Tipi di Lock

- **Read Lock (Shared)**: Multipli reader possono acquisire contemporaneamente
- **Write Lock (Exclusive)**: Solo un writer alla volta, blocca anche i reader

### Lock Manager

Ogni chiave ha uno stato di lock gestito centralmente:

```rust
struct LockState {
    read_locks: HashSet<u64>,   // Transaction IDs con read lock
    write_lock: Option<u64>,    // Transaction ID con write lock
}
```

### Regole di Acquisizione

- **Read Lock**: Concesso se nessun write lock attivo (o se la tx ha già il write lock)
- **Write Lock**: Concesso solo se nessun altro lock attivo
- **Lock Upgrade**: Possibile passare da read a write lock

## Snapshot Isolation

NeuroIndex implementa **Snapshot Isolation** per bilanciare performance e consistenza:

### Caratteristiche

- **Read Snapshot**: Ogni transazione legge da uno snapshot consistente
- **No Dirty Reads**: Mai visibili dati non committati
- **No Non-Repeatable Reads**: Letture ripetute restituiscono stesso valore
- **Write Skew**: Possibile ma raro (trade-off accettabile)

### Implementazione

```rust
struct Transaction<K, V> {
    id: u64,
    read_set: HashMap<K, V>,      // Snapshot dei valori letti
    write_set: HashMap<K, V>,     // Scritture buffered
    delete_set: HashSet<K>,       // Cancellazioni buffered
    locks: Vec<(K, LockType)>,    // Lock acquisiti
    state: TransactionState,      // Active/Preparing/Committed/Aborted
}
```

## Commit e Rollback

### Commit Protocol

1. **Validation**: Verifica che la transazione sia in stato Active
2. **Preparing**: Transizione a stato Preparing
3. **Apply**: Applica write_set e delete_set atomicamente
4. **Committed**: Marca come Committed
5. **Release Locks**: Rilascia tutti i lock
6. **Cleanup**: Rimuove da active transactions

### Rollback

In caso di errore o abort esplicito:
1. **Abort State**: Marca transazione come Aborted
2. **Discard Changes**: Scarta write_set e delete_set
3. **Release Locks**: Rilascia tutti i lock
4. **Cleanup**: Rimuove da active transactions

### Esempio d'Uso

```rust
let tx_mgr = TransactionManager::new();

// Inizia transazione
let tx = tx_mgr.begin();

// Acquisisce lock e opera
tx_mgr.acquire_read_lock(&tx, &"account_a")?;
tx_mgr.acquire_write_lock(&tx, &"account_b")?;

// Buffer modifiche
tx.write().add_write("account_b", new_balance);

// Commit atomico
let (writes, deletes) = tx_mgr.commit(tx)?;

// Applica al database
for (k, v) in writes {
    engine.put(k, v)?;
}
```

### Garanzie ACID

- **Atomicity**: Tutte le operazioni o nessuna
- **Consistency**: Invarianti mantenuti
- **Isolation**: Snapshot Isolation + 2PL
- **Durability**: Combinato con WAL per persistenza

<div style="page-break-after: always;"></div>

# AUTO-TUNING E OTTIMIZZAZIONE

NeuroIndex include un sistema di **auto-tuning** che monitora continuamente le performance e fornisce raccomandazioni per l'ottimizzazione.

## Sistema di Auto-Tuning

### Architettura

Il sistema è composto da:

1. **PerfCounters**: Contatori atomici per metriche
2. **AutoTuner**: Analizzatore che genera raccomandazioni
3. **Background Thread**: Esegue analisi periodiche (ogni 60s)

### Metriche Monitorate

```rust
pub struct PerfCounters {
    get_ops: AtomicU64,
    put_ops: AtomicU64,
    delete_ops: AtomicU64,
    
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    
    cuckoo_kicks: AtomicU64,
    cuckoo_stash_uses: AtomicU64,
    cuckoo_resizes: AtomicU64,
    
    ttree_rotations: AtomicU64,
    ttree_splits: AtomicU64,
}
```

## Metriche e Raccomandazioni

### Categorie di Analisi

1. **Cache Efficiency**
   - Target: 95% hit rate
   - Raccomandazione: Aumentare shard count, enable prefetching

2. **Hash Table Efficiency**
   - Target: < 10 kicks per insert
   - Raccomandazione: Aumentare table size, migliorare hash functions

3. **Memory Allocation**
   - Target: < 10 resize in lifetime
   - Raccomandazione: Pre-allocare capacità maggiore

4. **CPU Optimization**
   - Target: SIMD enabled su x86_64
   - Raccomandazione: Abilitare AVX2/SSE4.2

### Esempio Output

```
=== Auto-Tuning Recommendations ===
[High] CacheEfficiency: Cache hit rate (80.00%) is below target (95.00%). 
  Consider: 1) Increasing shard count to improve locality, 
  2) Enabling prefetching for range queries, 
  3) Using smaller key/value sizes for better cache line utilization

[Medium] HashTableEfficiency: High cuckoo kick rate (12.50 kicks/op). 
  Consider: 1) Increasing table size, 
  2) Using better hash functions, 
  3) Increasing bucket size from 4 to 8
===================================
```

### Attivazione

```rust
let engine = Arc::new(Engine::with_shards(4, 16));

// Abilita auto-tuning
Engine::enable_auto_tuning(&engine);

// Il sistema ora analizza performance ogni 60s
// e logga raccomandazioni automaticamente
```

### Integrazione con Prometheus

Il sistema espone metriche in formato Prometheus:

```
neuroindex_operations_total{operation="get"} 1000000
neuroindex_cache_hit_rate 0.95
neuroindex_memory_bytes 1073741824
neuroindex_cuckoo_kicks_total 1250
```

Questo permette integrazione con:
- **Grafana**: Dashboard visuali
- **Alertmanager**: Alert automatici
- **Prometheus**: Time-series storage

