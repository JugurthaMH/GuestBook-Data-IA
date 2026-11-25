Problème 1 - Casse et espaces en trop :
```
Jeffery Wells,jeffery.wells@example.com,fr 
```

```
Dr. Ana White DVM,dr..ana.white.dvm@example.com,FR
```

---

Problème 2 - Construit à partir du nom : les titres (Mr., Dr.) et suffixes (Jr.) donnent des points doublés ou un point avant @ :
```
Dr. Ana White DVM,dr..ana.white.dvm@example.com,FR
```

```
Joseph Webb Jr.,joseph.webb.jr.@example.com,ES
```

---

Problème 3 - Trois formats de date :
```
1540,289,2026-11-23,201.92,EUR
```

```
1521,284,22/11/2026,232.54,EUR
```

```
1500,129,20261120,19.25,usd
```
---

Problème 4 - Casse, et des montants en dollars à convertir :
```
522:1502,373,20261120,54.71,EUR
```

```
528:1508,38,20261121,27.8,usd
```

---

Problème 5 - Deux lignes identiques :

```
353:1343,60,20261110,152.26,usd
354:1343,60,20261110,152.26,usd
```