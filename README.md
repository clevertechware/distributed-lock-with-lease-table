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

Le second ne se contente pas de ne rien faire : il dit pourquoi. Le `fence` avance à chaque revendication, y compris
après une libération propre : 1, puis 2, puis 3 au fil des jobs de `replica-a`.

```
time=…T15:19:14 level=INFO msg="job started" replica=replica-a job=invoices fence=1
time=…T15:19:14 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T15:19:16 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T15:19:19 level=INFO msg="job done" replica=replica-a job=invoices fence=1
time=…T15:19:19 level=INFO msg="job started" replica=replica-a job=invoices fence=2
time=…T15:19:24 level=INFO msg="job done" replica=replica-a job=invoices fence=2
time=…T15:19:24 level=INFO msg="job started" replica=replica-a job=invoices fence=3
```

Un `Ctrl-C` sur `replica-a` libère le bail au lieu de le laisser expirer, y compris en plein job :
`replica-b` prend le relais au tic suivant, sans attendre les six secondes.

### Ce que seul le bail rattrape : `kill -9`

Un replica tué net ne libère rien. Personne ne travaille jusqu'à l'expiration du bail, puis
`replica-b` le reprend. Le `fence` avance d'un cran, comme à chaque revendication ; la reprise se lit à
l'absence de `job done` du porteur précédent :

```
time=…T15:19:56 level=INFO msg="job started" replica=replica-a job=invoices fence=2
time=…T15:19:59 (kill -9 replica-a, en plein job, fence=2)
time=…T15:20:01 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T15:20:03 level=INFO msg="job skipped, another replica holds it" replica=replica-b job=invoices
time=…T15:20:05 level=INFO msg="job started" replica=replica-b job=invoices fence=3
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
| `TestClaimingItsOwnValidLeaseAgainMovesTheFence` | La revendication reste permise au porteur courant, et c'est un nouveau mandat : le `fence` avance. |
| `TestLateReleaseOfPreviousMandateLeavesTheReclaimedLeaseUntouched` | Une libération tardive du mandat précédent, revendiqué entre-temps par le même porteur sur un bail encore valide, renvoie `ErrLost` et laisse le bail courant vivant. |
| `TestRenewByThePreviousMandateTouchesNoRowOnceTheLeaseIsReclaimed` | Le battement du mandat précédent ne prolonge rien une fois le bail revendiqué. |
| `TestClaimTakesOverAnExpiredLeaseAndMovesTheFence` | Un bail expiré se reprend, et le `fence` avance : la discontinuité est signalée. |
| `TestHeartbeatKeepsTheLeaseAliveWellBeyondItsDuration` | Le battement prolonge le bail bien au-delà de sa durée, sans allonger celle-ci. |
| `TestRenewFailsOnceTheLeaseHasBeenTakenOver` | On ne prolonge pas en douce un bail qu'on a laissé filer. |
| `TestUnlockLeavesTheLeaseOfTheReplicaThatTookOverUntouched` | Un `Unlock` tardif ne supprime pas le bail du replica qui travaille légitimement. |
| `TestUnlockFromAPreviousRunOfTheSameReplicaLeavesTheCurrentLeaseUntouched` | Un `Unlock` d'une exécution précédente, après expiration, ne touche pas le bail revendiqué depuis par le même porteur : le `fence` diffère. |
| `TestCleanReleaseThenReclaimMovesTheFence` | La libération fait expirer le bail sur place au lieu de supprimer la ligne : la revendication suivante donne le `fence` 2. |
| `TestReplayedReleaseOfPreviousRunLeavesCurrentLeaseUntouched` | Une libération rejouée après une libération propre et une nouvelle revendication du même porteur renvoie `ErrLost`, et le bail courant reste vivant. |
| `TestReleasingALeaseShorterThanASecondRespectsTheValidityConstraint` | La libération d'un bail de moins d'une seconde ne viole pas `lease_validity`. |
| `TestReclaimingAnExpiredLeaseInPlaceRespectsTheValidityConstraint` | La revendication d'un bail expiré sur la même ligne garde `expires_at` après `acquired_at`. |
| `TestDownstreamFenceAcceptsTheNewHolderAfterACleanRelease` | Une ressource aval qui applique `WHERE last_fence < $1` accepte le nouveau porteur après une libération propre, et rejette l'ancien jeton. |
| `TestClaimingALeaseKeepsNoConnectionOutOfThePool` | La connexion repart au pool aussitôt la revendication répondue. |
| `TestSchemaRejectsALeaseAlreadyExpiredAtBirth` | La contrainte `lease_validity` transforme une erreur de calcul de durée en violation de contrainte. |
| `TestMigratePurgesALeaseExpiredForLongerThanTheRetention` | La libération conserve la ligne : `Migrate` supprime au démarrage les baux expirés depuis plus de 24 h, pour que les clés des jobs disparus ne s'accumulent pas. |
| `TestMigrateKeepsALeaseExpiredForLessThanTheRetentionAndItsFence` | Un bail expiré depuis moins de 24 h garde sa ligne et son `fence`. |
| `TestMigrateKeepsALiveLease` | La purge ne touche pas un bail en cours. |
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
| `TestTheFirstTwoConsecutiveClaimFailuresAreWarningsAndTheThirdIsAnError` | Les deux premiers échecs consécutifs de la revendication sont journalisés en WARN, le troisième et les suivants en ERROR. |
| `TestASuccessfulClaimRestartsTheFailureSeries` | Une revendication réussie remet la série à zéro. |
| `TestAClaimRefusedBecauseAnotherReplicaHoldsTheLockRestartsTheFailureSeries` | `ErrAlreadyLocked` remet la série à zéro. |
| `TestACanceledClaimIsNotLoggedAndRestartsTheFailureSeries` | `context.Canceled` n'est pas journalisé et remet la série à zéro. |
| `TestAClaimThatExceededItsDeadlineCountsInTheFailureSeries` | `context.DeadlineExceeded` compte dans la série : un locker trop lent doit apparaître. |
| `TestEveryClaimFailureIsAnErrorWhenTheDegradationIsDisabled` | `WithoutClaimFailureDegradation` rétablit un ERROR par échec. |
| `TestTheClaimFailureLogCarriesTheLengthOfTheSeries` | Le journal porte l'attribut `streak`. |

## Changements

Trois changements font suite aux articles
[*Le lease en production : ce que gocron avale*](https://www.clevertechware.fr/blog/posts/2026/le-lease-en-production-ce-que-gocron-avale)
et
[*Libérer un bail sans perdre le fence*](https://www.clevertechware.fr/blog/posts/2026/le-lease-en-production-liberer-sans-perdre-le-fence).

- **Libération.** `Unlock` fait expirer le bail par un `UPDATE` au lieu de supprimer la ligne. Avec le `DELETE`, la
  ligne était réinsérée avec `fence = 1` à chaque revendication suivante, et une libération rejouée d'une exécution
  précédente supprimait le bail de l'exécution courante. Le `fence` croît désormais à chaque revendication, y compris celle d'un porteur qui
  revendique son propre bail encore valide : `holder` identifie le replica, `fence` identifie le mandat.
- **Purge.** La ligne survit à la libération, donc la clé d'un job supprimé resterait indéfiniment. `Migrate` supprime
  au démarrage les baux expirés depuis plus de 24 h (`Retention`). Une clé purgée puis réutilisée repart à `fence = 1`.
- **Scheduler.** Les deux premiers échecs consécutifs de la revendication sont journalisés en WARN, le troisième en
  ERROR. `WithoutClaimFailureDegradation` rétablit un ERROR par échec.

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
borne la fenêtre de violation et expose un `fence`, qui croît à chaque revendication ; la garantie n'existe que si la ressource écrite
en aval mémorise le dernier jeton vu et rejette tout ce qui arrive avec un jeton inférieur. Cette
vérification en aval est hors du périmètre de la démonstration, et c'est pourquoi l'article insiste
sur l'idempotence des jobs.
