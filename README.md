# Quand l'advisory lock ne suffit plus : passer à un lease

Projet de démonstration accompagnant l'article
[*Quand l'advisory lock ne suffit plus : passer à un lease*](https://clevertechware.fr/blog/posts/2026/quand-l-advisory-lock-ne-suffit-plus).

La démonstration, ce sont les **tests**. Chacun exécute le scénario décrit dans l'article sur un vrai
PostgreSQL démarré par [testcontainers](https://golang.testcontainers.org/), et vérifie le
comportement au lieu de le raconter. Pour le voir de ses yeux plutôt que de le lire, `cmd/replica`
rejoue les mêmes scénarios dans deux terminaux.

## Lancer la démonstration

Un démon Docker en écoute est nécessaire, tout le reste est géré par les tests.

```bash
make test    # go test ./... -race
make all     # formatage, vet, lint et tests
```

## Voir la différence — `cmd/replica`

`cmd/replica` est une application qui doit exécuter un job sur un seul replica à la fois : à chaque
tic, elle réclame le verrou, travaille si elle l'a obtenu, et le rend. Deux instances contre le même
PostgreSQL, et le journal raconte le reste.

```bash
make demo-db                                    # un PostgreSQL jetable sur localhost:5432
go run ./cmd/replica -name replica-a            # terminal 1
go run ./cmd/replica -name replica-b            # terminal 2
make demo-db-down                               # une fois la démonstration terminée
```

Les deux cibles enveloppent le `compose.yaml` de la racine — `docker compose up -d --wait` et
`docker compose down` font aussi bien l'affaire. La base n'a pas de volume : chaque démonstration
part d'une table `lease` vide.

| Drapeau | Défaut | Rôle |
|---|---|---|
| `-name` | le nom d'hôte et le pid | l'identité du replica, et le porteur du bail |
| `-job` | `invoices` | la clé du job que les replicas se disputent |
| `-locker` | `lease` | `lease`, ou `advisory` pour l'implémentation fautive |
| `-lease` | `6s` | la durée que couvre un bail |
| `-interval` | `2s` | l'intervalle entre deux tentatives de revendication |
| `-work` | `5s` | la durée du job |

La connexion se règle par `DATABASE_URL`, et vise `make demo-db` à défaut.

### Avec le bail : un seul replica travaille

Le second ne se contente pas de ne rien faire : il dit pourquoi.

```
time=…T22:17:40 level=INFO msg="job started" replica=replica-a job=invoices fence=1
time=…T22:17:40 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T22:17:42 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T22:17:45 level=INFO msg="job done" replica=replica-a job=invoices fence=1
```

Un `Ctrl-C` sur `replica-a` libère le bail au lieu de le laisser expirer, y compris en plein job :
`replica-b` prend le relais au tic suivant, sans attendre les six secondes.

### Ce que seul le bail rattrape : `kill -9`

Un replica tué net ne libère rien. Personne ne travaille jusqu'à l'expiration du bail, puis
`replica-b` le reprend — et le `fence` a bougé, ce qui est précisément le signal d'une reprise :

```
time=…T22:21:24 (kill -9 replica-a, en plein job, fence=1)
time=…T22:21:26 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T22:21:28 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T22:21:30 level=INFO msg="job started" replica=replica-b job=invoices fence=2
```

Six secondes de job non exécuté : c'est le prix du `-lease`, et le seul chiffre à régler.

### Sans le bail : `-locker advisory`

La même application sur l'implémentation de l'article, avec un `ConnMaxLifetime` d'un tic pour que
le pool recycle ses connexions comme il le fait en production à l'échéance :

```bash
go run ./cmd/replica -locker advisory -name replica-a
go run ./cmd/replica -locker advisory -name replica-b
```

Le verrou disparaît avec la connexion qui le portait, sans un mot, et les deux replicas travaillent
en même temps :

```
time=…T22:20:37 level=INFO msg="job started" replica=replica-a job=invoices
time=…T22:20:39 level=INFO msg="job started" replica=replica-b job=invoices   ← les deux
time=…T22:20:42 level=INFO msg="job done"    replica=replica-a job=invoices
time=…T22:20:44 level=INFO msg="job done"    replica=replica-b job=invoices
```

## Ce que chaque test démontre

### Le verrou incriminé — `internal/advisory`

`SessionLocker` est le code de l'article : un `pg_try_advisory_lock` posé à travers un `*sql.DB`,
c'est-à-dire à travers un **pool**, et non une connexion.

| Test | Ce qu'il prouve |
|---|---|
| `TestSessionLockerLeavesTheLockBehindWhenUnlockRunsOnAnotherConnection` | La **défaillance bruyante** : le `Unlock` part sur une autre connexion, PostgreSQL renvoie le `WARNING: you don't own a lock of type ExclusiveLock` (capté par le test), et le verrou reste accroché à la connexion d'origine. |
| `TestOrphanedLockIsSilentlyReleasedWhenThePoolRecyclesTheConnection` | La **défaillance silencieuse** : quand le pool recycle la connexion, PostgreSQL libère le verrou sans un mot, et un second replica démarre le même job. |
| `TestPinnedLockerReleasesTheLockItAcquired` | Le premier correctif : sur une connexion épinglée, la sémantique est correcte et le warning disparaît. |
| `TestPinnedLockerKeepsItsConnectionOutOfThePoolForTheWholeJob` | Ce que coûte ce correctif : la connexion est confisquée pendant toute la durée du job. |

Les deux ressorts de la reproduction sont volontairement déterministes : `holdIdleConnection` occupe
la connexion inactive du pool pour forcer la requête suivante sur une autre, et
`recycleIdleConnections` ferme les connexions inactives, comme le pool le fait de lui-même à
l'échéance de `ConnMaxLifetime`.

### Le bail — `internal/lease`

Le verrou n'est plus un état de connexion mais une ligne : un porteur, une expiration, un
*fencing token*.

| Test | Ce qu'il prouve |
|---|---|
| `TestClaimIsRefusedWhileAnotherReplicaHoldsTheLease` | Un bail valide détenu par un autre porteur ne se vole pas : le `RETURNING` ne rend aucune ligne. |
| `TestClaimingItsOwnValidLeaseAgainKeepsTheFence` | La revendication est réentrante : le porteur courant ne se vole pas son propre bail. |
| `TestClaimTakesOverAnExpiredLeaseAndMovesTheFence` | Un bail expiré se reprend, et le `fence` avance : la discontinuité est signalée. |
| `TestHeartbeatKeepsTheLeaseAliveWellBeyondItsDuration` | Le battement prolonge le bail bien au-delà de sa durée, sans allonger celle-ci. |
| `TestRenewFailsOnceTheLeaseHasBeenTakenOver` | On ne prolonge pas en douce un bail qu'on a laissé filer. |
| `TestUnlockLeavesTheLeaseOfTheReplicaThatTookOverUntouched` | Un `Unlock` tardif ne supprime pas le bail du replica qui travaille légitimement. |
| `TestUnlockFromAPreviousRunOfTheSameReplicaLeavesTheCurrentLeaseUntouched` | Le dernier interstice, celui que seul le `fence` ferme : même clé, même porteur, exécution précédente. |
| `TestClaimingALeaseKeepsNoConnectionOutOfThePool` | La connexion repart au pool aussitôt la revendication répondue. |
| `TestSchemaRejectsALeaseAlreadyExpiredAtBirth` | La contrainte `lease_validity` transforme une erreur de calcul de durée en violation de contrainte. |
| `TestMigrateRunsConcurrentlyOnEveryReplica` | La création de la table reste sérialisée par un `pg_advisory_xact_lock` : le bon outil, à la bonne portée. |

### La boucle du replica — `internal/scheduler`

Ce que `cmd/replica` exécute, et donc ce que la démonstration à deux terminaux vérifie sans le
regard d'un lecteur.

| Test | Ce qu'il prouve |
|---|---|
| `TestOnlyOneOfTwoReplicasRunsTheJobAtATime` | Deux replicas sur le même job, et le travail ne se chevauche jamais. |
| `TestTheStandbyReplicaTakesOverOnlyOnceTheHolderStops` | Le second attend son tour tant que le premier travaille, et le prend dès qu'il s'arrête. |
| `TestAFailingJobStillGivesTheLockBack` | Un job qui échoue rend le verrou : l'échec ne bloque pas les tours suivants. |
| `TestAJobInterruptedByTheShutdownIsNotReportedAsAFailure` | Un arrêt en plein job n'est pas une panne, et n'a rien à faire dans les erreurs. |

## Organisation

```
cmd/replica         le binaire de démonstration : deux instances, un job, un verrou
pkg/lock            le contrat attendu par l'ordonnanceur : Lock, Unlock, ErrAlreadyLocked
internal/advisory   le verrou incriminé, et sa correction par épinglage de connexion
internal/lease      le bail : revendication, battement, libération, migration du schéma
internal/scheduler  la boucle d'un replica : réclamer, travailler, libérer, recommencer
internal/pgtest     le PostgreSQL jetable et la capture des messages serveur
```

## Ce que ce projet ne fait pas

Le bail **ne garantit pas l'exclusion mutuelle**, pas plus que l'advisory lock qu'il remplace. Il
borne la fenêtre de violation et expose un `fence` ; la garantie n'existe que si la ressource écrite
en aval mémorise le dernier jeton vu et rejette tout ce qui arrive avec un jeton inférieur. Cette
vérification en aval est hors du périmètre de la démonstration, et c'est pourquoi l'article insiste
sur l'idempotence des jobs.
