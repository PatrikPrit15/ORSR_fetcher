# Inštalácia

## Ako prvé je treba nainštalovať potrebné knižnice.

``` pip3 install -r req.txt ```  alebo  ``` pip install -r req.txt ```

## Následne MongoDB.

# Spustenie

## Štart mongodb:

```sudo systemctl start mongod```

## Štart programu a fetch dát:

```python3 scraper.py``` alebo ```pypy3 scraper.py``` alebo ```python scraper.py```

Časy - python3-30s, pypy3-40s  (časy sú dosť premenlivé kvôli nespoľahlivosti/lagu serveru)


## Štart api:

```python3 api.py``` alebo ```python scraper.py```

Api má endpointy detail (s parametrom ico - add/detail/\<ico\>) a
 list.

Detail vráti detaily spoločnosti a list vráti všetky ico a obchodné mená spoločností.