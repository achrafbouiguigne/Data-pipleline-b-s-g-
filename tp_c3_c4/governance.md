# Gouvernance & Sécurité — Pipeline Sales Events

## 1. SLA & SLO

### SLA (engagement contractuel)
| Indicateur        | Engagement                                      |
|-------------------|-------------------------------------------------|
| Disponibilité Gold| Tables Gold dispo chaque jour à 08h00 UTC       |
| Fraîcheur         | Données jusqu'à now - 1h maximum                |
| Taux d'erreur     | < 1 run FAIL sur 100 (30j glissants)            |

### SLO (objectifs internes)
| Objectif          | Seuil                                           |
|-------------------|-------------------------------------------------|
| Durée pipeline    | ≤ 30 minutes                                    |
| Taux quarantaine  | < 2% des lignes ingérées                        |
| Volume anomaly    | Alerte si delta ±20%                            |

## 2. Règles Sécurité / PII

1. **RBAC Least Privilege** — chaque job n'accède qu'à ses dossiers
2. **Chiffrement au repos** — AES-256 sur le stockage objet (S3/GCS)
3. **Pas de secrets dans le code** — variables d'env ou Vault
4. **Logs sans PII** — uniquement métriques agrégées dans logs/
5. **Rétention** — Bronze 90j, Silver 2 ans, Gold 5 ans, Quarantine 30j
6. **Audit trail** — tout accès en écriture loggé avec user+timestamp+op