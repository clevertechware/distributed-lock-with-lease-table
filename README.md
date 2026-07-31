# Quand l'advisory lock ne suffit plus : passer à un lease

Projet de démonstration accompagnant l'article
[*Quand l'advisory lock ne suffit plus : passer à un lease*](https://clevertechware.fr/blog/posts/2026/quand-l-advisory-lock-ne-suffit-plus).

La démonstration, ce sont les **tests**. Chacun exécute le scénario décrit dans l'article sur un vrai
PostgreSQL démarré par [testcontainers](https://golang.testcontainers.org/), et vérifie le
comportement au lieu de le raconter.

## Lancer la démonstration

Un démon Docker en écoute est nécessaire, tout le reste est géré par les tests.

```bash
make test    # go test ./... -race
make all     # formatage, vet, lint et tests
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

## Organisation

```
internal/lock       le contrat attendu par l'ordonnanceur : Lock, Unlock, ErrAlreadyLocked
internal/advisory   le verrou incriminé, et sa correction par épinglage de connexion
internal/lease      le bail : revendication, battement, libération, migration du schéma
internal/pgtest     le PostgreSQL jetable et la capture des messages serveur
```

## Ce que ce projet ne fait pas

Le bail **ne garantit pas l'exclusion mutuelle**, pas plus que l'advisory lock qu'il remplace. Il
borne la fenêtre de violation et expose un `fence` ; la garantie n'existe que si la ressource écrite
en aval mémorise le dernier jeton vu et rejette tout ce qui arrive avec un jeton inférieur. Cette
vérification en aval est hors du périmètre de la démonstration, et c'est pourquoi l'article insiste
sur l'idempotence des jobs.
