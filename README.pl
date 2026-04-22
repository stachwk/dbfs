# DBFS

[![CI](https://github.com/stachwk/dbfs/actions/workflows/ci.yml/badge.svg)](https://github.com/stachwk/dbfs/actions/workflows/ci.yml) [Roadmap](ROADMAP.md) [Benchmarks](BENCHMARKS.md)

DBFS to filesystem oparty o PostgreSQL, wystawiany przez FUSE. Ma zachowywać się jak praktyczny filesystem Linuksa: z przewidywalnymi metadanymi, sensowną semantyką katalogów, advisory locking, access checkami świadomymi ACL oraz testami, które sprawdzają realne ścieżki wykonania od końca do końca.

Projekt skupia się na:

- stabilnych metadanych filesystemu
- sensownej zgodności z Linux/VFS
- jawnych opcjach runtime dla SELinux, ACL i polityki `atime`
- testach integracyjnych, które sprawdzają rzeczywiste zachowanie mounta, a nie tylko backend helpery

## Aktualny Stan

- Główne operacje FUSE są zaimplementowane i pokryte testami integracyjnymi.
- `make test-all` przechodzi, a `make test-all-full` jest dostępne jako szerszy zestaw.
- Odczyty korzystają teraz z blokowego ładowania z małym cache i read-ahead zamiast pełnego ładowania pliku przy każdym dostępie.
- Warstwa lookup/namespace została wydzielona do `dbfs_namespace.py`, a logika repository jest teraz w `mod/repository/` jako wrapper oraz `lookup.py`, `attrs_listing.py`, `create.py`, `delete.py` i `mutations.py`, więc główny moduł FUSE nie trzyma już bezpośrednio logiki rozwiązywania ścieżek, ID ani CRUD dla namespace. Rozwiązywanie katalogów używa teraz cache'owanych rekursywnych CTE, a rozwiązywanie wpisów jest zebrane tak, by zmniejszyć liczbę round-tripów do namespace.
- Warstwa metadanych/zapytań oraz krótkie cache TTL zostały wydzielone do `dbfs_metadata.py`, logika dopisywania do journala żyje teraz w `dbfs_journal.py`, polityka uprawnień/własności została wydzielona do `dbfs_permissions.py`, a walidacja mount/runtime żyje teraz w `dbfs_runtime_validation.py`, więc główny moduł FUSE nie trzyma już bezpośrednio tych warstw helperów.
- SELinux działa jako xattr z runtime gating; pełna polityka mount-label jest celowo poza zakresem.
- PostgreSQL TLS jest opcjonalny i konfigurowalny; DBFS może też wygenerować lokalną parę certyfikat/klucz na żądanie.
- Przejściowe zerwania połączenia PostgreSQL w gorącej ścieżce odczytu/zapisu są ponawiane raz, z zachowaniem stanu po stronie procesu klienta, więc aktywny dirty write state i cache odczytu mogą przetrwać próbę reconnect.
- Migracja lock managera już się dokonała: PostgreSQL-backed leases są produkcyjną ścieżką dla zarówno `flock`, jak i range-locków `fcntl`, z TTL i heartbeat. `make test-locking` pozostaje zestawem semantyki locków, a `make test-pg-lock-manager` pokrywa produkcyjny backend PostgreSQL, w tym regresję dla dwóch klientów piszących do tego samego pliku, która pokazuje że DBFS nie pozwala im rozjechać zapisów chronionych lockiem.
- `make test-pg-lock-manager` pokrywa produkcyjny backend locków oparty o PostgreSQL, w tym regresję dla dwóch klientów piszących do tego samego pliku, która pokazuje że DBFS nie pozwala im rozjechać zapisów chronionych lockiem.
- Zmiany schematu żyją teraz w `migrations/` z sekwencyjnymi wersjami, jawnym eksportem `mkfs.dbfs.py status` i ścieżką upgrade ze starszych stanów schematu.
- Obecna wersja DBFS jest zdefiniowna w dbfs_version.py, a `dbfs_bootstrap.py --version` i `mkfs.dbfs.py --version` wypisują tę samą wartość.
- Prace nad wydajnością są już w kodzie, a aktualne baseline'y benchmarków są zapisane w `BENCHMARKS.md`.
- Lokalny stack Docker Compose preloaduje `pg_stat_statements`, więc analiza zapytań i profilowanie runtime mogą korzystać z trwałych statystyk PostgreSQL.
- `TODO.md` służy teraz jako log decyzji i notatek, a nie aktywny backlog implementacyjny.

## Pokrycie CI

Workflow GitHub Actions uruchamia krótki job kompilacyjny oraz wybrany matrix testów:

| Job | Co robi |
| --- | --- |
| `compile` | Byte-compiluje moduły core oraz obecne entry pointy testowe. |
| `workflow runtime` | Wymusza Node 24 dla akcji JavaScript przed domyślną zmianą GitHuba. |
| `test-runtime-config` | Sprawdza parsowanie runtime config i wynikowe wartości strojenia. |
| `test-runtime-validation` | Sprawdza, że błędne wartości runtime fail-fast odrzucają start. |
| `test-runtime-profile` | Sprawdza nazwane profile runtime. |
| `test-schema-upgrade` | Sprawdza bezpieczne `init` schematu, naprawę wersji i ochronę sekretu administracyjnego schematu. |
| `test-schema-status` | Sprawdza eksport statusu schematu i udokumentowany manifest migracji. |
| `test-postgresql-requirements` | Sprawdza minimalną wersję PostgreSQL i pojemność połączeń. |
| `test-metadata-cache` | Sprawdza krótki TTL cache metadanych i `statfs`. |
| `test-pg-lock-manager` | Sprawdza PostgreSQL-backed lock backend i regresję dla dwóch klientów piszących do tego samego pliku. |
| `test-read-ahead-sequence` | Sprawdza sekwencyjny read-ahead. |
| `test-block-read` | Sprawdza odczyt zakresowy bloków zamiast pełnego pliku. |
| `test-flush-release-profile` | Sprawdza zachowanie profilowania `flush/release`. |

## Znane Ograniczenia

- Pełna polityka mount-label SELinux jest celowo poza zakresem; DBFS trzyma SELinux jako metadane w xattr plus runtime gating.
- Obsługa `ioctl` jest celowo ograniczona na razie do `FIONREAD`.
- Metadane specjalnych urządzeń są zapisywane, ale pełna semantyka uruchamiania takich node'ów nie jest głównym celem projektu.
- `make test-all` jest głównym targetem regresji; workflow mounta są pokryte, ale CI skupia się na wybranym zestawie stabilnym w automatyzacji.
- Upgrade schematu jest na razie zachowawczy: `init` jest idempotentne i nie niszczy istniejących obiektów, `upgrade` naprawia brakujący stan schematu i przywraca bieżącą wersję, ale repo nie ma jeszcze długiego łańcucha plików migracji.
- DBFS normalizuje timestampy przez sesję PostgreSQL ustawioną na UTC oraz konwersje po stronie Pythona, więc lokalne różnice stref czasowych nie przesuwają metadanych. Ustawienie UTC jest inicjalizowane raz na fizyczne połączenie z puli, a nie przy każdej operacji filesystemu, i nie opiera się na domyślnych ustawieniach tworzenia bazy.
- Recovery jest ograniczone do ponawiania przejściowych disconnectów w gorącej ścieżce odczytu/zapisu; DBFS trzyma stan dirty i cache w pamięci procesu, ale nie robi jeszcze pełnego replay dowolnych trwających operacji SQL.

Licencja: MIT

## Wymagania

- Python 3
- `fusepy` (`pip install fusepy`)
- `psycopg2` albo `psycopg2-binary`
- PostgreSQL
- wsparcie FUSE na hoście
- `openssl`, jeśli DBFS ma automatycznie generować parę certyfikat/klucz TLS dla PostgreSQL

## Pakiet Pip

DBFS można zainstalować do virtualenv przez pip:

```bash
make venv
make pip-install-editable
```

To instaluje skrypty projektu do aktywnego venv:

- `dbfs-bootstrap`
- `mkfs.dbfs`
- `mount.dbfs`

W katalogu źródłowym nadal zostają bezpośrednie skrypty `dbfs_bootstrap.py` i `mkfs.dbfs.py`; pakiet pip instaluje krótsze nazwy poleceń powyżej. Jeśli chcesz instalację bez trybu editable, użyj `make pip-install`. Editable działa przez `make pip-install-editable`, jeśli venv widzi `setuptools`. Metadane pakietu są w `setup.py`.
Zainstalowany `mount.dbfs` najpierw wybiera `.venv/bin/dbfs-bootstrap` z bieżącego projektu, potem `dbfs-bootstrap` z `PATH`. Jeśli `DBFS_CONFIG` nie jest ustawione, a w bieżącym katalogu istnieje lokalny `./dbfs_config.ini`, wrapper eksportuje go automatycznie. Jeśli nie znajdzie żadnego poprawnego bootstrappera ani sensownego pliku konfiguracyjnego, kończy się jasnym komunikatem zamiast zgadywać interpreter Pythona.

Przykład:

```bash
mount.dbfs /mnt/dbfs
```

Jeśli chcesz nazwany profil runtime, ustaw `DBFS_PROFILE` jawnie albo podaj `--profile` / `-o profile=...` wtedy, gdy naprawdę potrzebujesz strojenia pod konkretny workload.

Wymagania PostgreSQL dla obecnego zestawu funkcji:

- PostgreSQL 9.5 lub nowszy
- `max_connections` powinno być wyraźnie większe niż `pool_max_connections`; jako praktyczne minimum zostaw co najmniej dwa dodatkowe połączenia dla administracji i równoległych klientów DBFS
- nie są potrzebne specjalne parametry lock managera; domyślne `read committed` wystarcza
- DBFS oczekuje transakcyjnych połączeń PostgreSQL z wyłączonym `autocommit`
- DBFS inicjalizuje stan sesji UTC raz na fizyczne połączenie z puli i w stanie ustalonym zostaje tylko tani `rollback()`
- `sslmode=require` wystarcza do szyfrowania połączenia, a `verify-full` jest właściwe, jeśli chcesz też weryfikację certyfikatu

| Wymaganie | Wartość |
| --- | --- |
| Wersja PostgreSQL | `9.5+` |
| Tryb transakcyjny | `autocommit = off` |
| Poziom izolacji | `read committed` |
| `max_connections` | `pool_max_connections + 2` lub więcej |
| TLS | `sslmode=require` do szyfrowania, `verify-full` do weryfikacji certyfikatu |

## Przykładowy `dbfs_config.ini`

To jest minimalny punkt startowy:

```ini
[database]
host = 127.0.0.1
port = 5432
dbname = dbfsdbname
user = dbfsuser
password = cichosza

[dbfs]
pool_max_connections = 10
synchronous_commit = on
write_flush_threshold_bytes = 67108864
read_cache_blocks = 1024
read_ahead_blocks = 4
sequential_read_ahead_blocks = 8
small_file_read_threshold_blocks = 8
workers_read = 4
workers_read_min_blocks = 8
workers_write = 4
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 1
statfs_cache_ttl_seconds = 2

[dbfs.profile.bulk_write]
write_flush_threshold_bytes = 268435456
read_cache_blocks = 512
read_ahead_blocks = 2
sequential_read_ahead_blocks = 4
small_file_read_threshold_blocks = 4
workers_read = 4
workers_read_min_blocks = 8
workers_write = 8
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 2
statfs_cache_ttl_seconds = 2

[dbfs.profile.metadata_heavy]
write_flush_threshold_bytes = 67108864
read_cache_blocks = 1024
read_ahead_blocks = 4
sequential_read_ahead_blocks = 8
small_file_read_threshold_blocks = 8
workers_read = 4
workers_read_min_blocks = 8
workers_write = 4
workers_write_min_blocks = 8
metadata_cache_ttl_seconds = 5
statfs_cache_ttl_seconds = 5

[dbfs.profile.pg_locking]
lock_backend = postgres_lease
lock_lease_ttl_seconds = 30
lock_heartbeat_interval_seconds = 10
lock_poll_interval_seconds = 0.05
```

## Pierwsze uruchomienie

Jeżeli uruchamiasz DBFS pierwszy raz, zrób to w takiej kolejności:

1. Zainstaluj zależności wymienione wyżej.
1. Przygotuj PostgreSQL i upewnij się, że użytkownik oraz hasło w `dbfs_config.ini` są poprawne.
1. Wybierz, skąd DBFS ma czytać konfigurację:
   - `/etc/dbfs/dbfs_config.ini`
   - albo lokalny plik `./dbfs_config.ini`
1. Utwórz schemat:

   ```bash
   python3 mkfs.dbfs.py init
   ```

1. Zamontuj filesystem:

   ```bash
   python3 dbfs_bootstrap.py -f /ścieżka/do/mountpointu
   ```

1. Zapisz plik do montażu, odczytaj go ponownie i sprawdź, czy dane przeżywają ponowne zamontowanie.
1. Po zakończeniu odmontuj filesystem:

   ```bash
   fusermount3 -u /ścieżka/do/mountpointu
   ```

## Minimalny start

Jeśli chcesz najszybszą drogę od zera do zamontowanego filesystemu, uruchom:

```bash
make up
make init
make mount
```

Jeśli chcesz użyć user-level pliku konfiguracyjnego zamiast `/etc/dbfs/dbfs_config.ini`, użyj:

```bash
make install-config-user
make mount-user
```

## Szybki start

1. Skonfiguruj `/etc/dbfs/dbfs_config.ini` albo lokalny `dbfs_config.ini`.
1. Opcjonalnie uruchom `make install-config`, żeby skopiować `dbfs_config.ini` do `/etc/dbfs/dbfs_config.ini`.
1. Dla lokalnego developmentu możesz uruchomić `make install-config-user`, żeby zainstalować `dbfs_config.ini` do `~/.config/dbfs/dbfs_config.ini` bez `sudo`.
1. `make config-show` pokazuje, którego pliku konfiguracyjnego DBFS użyje, a `make mount-user` wymusza user-level `~/.config/dbfs/dbfs_config.ini`.
1. Zainicjalizuj schemat:

   ```bash
   python3 mkfs.dbfs.py init
   ```

   Jeśli chcesz, żeby DBFS wygenerował lokalną parę certyfikat/klucz TLS PostgreSQL podczas tworzenia schematu, użyj:

   ```bash
   python3 mkfs.dbfs.py init --generate-client-tls-pair 1
   ```

   Ta sama opcja działa też z `upgrade`:

   ```bash
   python3 mkfs.dbfs.py upgrade --generate-client-tls-pair 1
   ```

1. Zamontuj filesystem:

   ```bash
   python3 dbfs_bootstrap.py -f /ścieżka/do/mountpointu
   ```

## Obsługiwane parametry

DBFS jest sterowany przez flagi CLI, zmienne środowiskowe oraz wartości z pliku konfiguracyjnego.

### Główne parametry runtime DBFS

| Parametr | Typ | Domyślnie | Efekt |
| --- | --- | --- | --- |
| `-f`, `--mountpoint` | CLI | wymagane | Punkt montowania filesystemu FUSE. |
| `--role auto|primary|replica` | CLI / `DBFS_ROLE` | `auto` | Steruje trybem tylko-do-odczytu dla repliki i autodetekcją roli. |
| `--selinux auto|on|off` | CLI / `DBFS_SELINUX` | `off` | Włącza lub wyłącza obsługę `security.selinux`. |
| `--acl on|off` | CLI / `DBFS_ACL` | `off` | Włącza lub wyłącza egzekwowanie POSIX ACL. |
| `--default-permissions` / `--no-default-permissions` | CLI / `DBFS_DEFAULT_PERMISSIONS` | on | Steruje tym, czy kernelowe sprawdzanie uprawnień jest aktywne. |
| `--atime-policy default|noatime|nodiratime|relatime|strictatime` | CLI / `DBFS_ATIME_POLICY` | `default` | Wybiera wewnętrzne zachowanie `atime` DBFS. |
| `--lazytime` | CLI / `DBFS_LAZYTIME` | off | Włącza opcję montowania `lazytime`. |
| `--sync` | CLI / `DBFS_SYNC` | off | Włącza opcję montowania `sync`. |
| `--dirsync` | CLI / `DBFS_DIRSYNC` | off | Włącza opcję montowania `dirsync`. |
| `DBFS_ALLOW_OTHER=1` | Zmienna środowiskowa | off | Włącza `allow_other`, jeśli FUSE na to pozwala. |
| `DBFS_DEBUG=1` | Zmienna środowiskowa | off | Włącza debugowy tryb montowania jako domyślny. |
| `DBFS_LOG_LEVEL=DEBUG|INFO|...` | Zmienna środowiskowa | `INFO` | Steruje poziomem logowania. |
| `DBFS_CONFIG` | Zmienna środowiskowa | auto-detekcja | Wymusza konkretną ścieżkę do pliku konfiguracyjnego. |
| `DBFS_SELINUX_CONTEXT` | Zmienna środowiskowa | nieustawione | Ustawia opcję mount `context=` dla SELinux. |
| `DBFS_SELINUX_FSCONTEXT` | Zmienna środowiskowa | nieustawione | Ustawia opcję mount `fscontext=` dla SELinux. |
| `DBFS_SELINUX_DEFCONTEXT` | Zmienna środowiskowa | nieustawione | Ustawia opcję mount `defcontext=` dla SELinux. |
| `DBFS_SELINUX_ROOTCONTEXT` | Zmienna środowiskowa | nieustawione | Ustawia opcję mount `rootcontext=` dla SELinux. |
| `DBFS_DEFAULT_PERMISSIONS` | Zmienna środowiskowa | `1` | Steruje tym, czy domyślne checki uprawnień są przekazywane do FUSE. |
| `DBFS_ENTRY_TIMEOUT_SECONDS` | Zmienna środowiskowa | `0` | Steruje TTL cache wpisów katalogu w FUSE. |
| `DBFS_ATTR_TIMEOUT_SECONDS` | Zmienna środowiskowa | `0` | Steruje TTL cache atrybutów w FUSE. |
| `DBFS_NEGATIVE_TIMEOUT_SECONDS` | Zmienna środowiskowa | `0` | Steruje TTL cache negatywnych wpisów w FUSE. |
| `DBFS_SYNCHRONOUS_COMMIT` | Zmienna środowiskowa | `on` | Steruje `synchronous_commit` PostgreSQL dla każdego połączenia. |
| `DBFS_PERSIST_BUFFER_CHUNK_BLOCKS` | Zmienna środowiskowa | `128` | Steruje liczbą dirty bloków pakowanych do jednego zapytania `persist_buffer()`. |
| `DBFS_PG_SSLMODE`, `DBFS_PG_SSLROOTCERT`, `DBFS_PG_SSLCERT`, `DBFS_PG_SSLKEY` | Zmienna środowiskowa | nieustawione | Nadpisuje parametry TLS połączenia do PostgreSQL. |

### Plik konfiguracyjny

`dbfs_config.ini` powinien zawierać sekcję `[database]` z parametrami połączenia do PostgreSQL:

- `host`
- `port`
- `dbname`
- `user`
- `password`
- `sslmode` dla szyfrowanego połączenia PostgreSQL, na przykład `require` albo `verify-full`
- `sslrootcert` dla certyfikatu CA używanego do weryfikacji serwera
- `sslcert` i `sslkey` dla opcjonalnej autoryzacji certyfikatem klienta

Może też zawierać sekcję `[dbfs]` z:

- `pool_max_connections`
- `write_flush_threshold_bytes`
- `read_cache_blocks`
- `read_ahead_blocks`
- `sequential_read_ahead_blocks`
- `small_file_read_threshold_blocks`
- `workers_read`
- `workers_read_min_blocks`
- `workers_write`
- `workers_write_min_blocks`
- `persist_buffer_chunk_blocks`
- `copy_skip_unchanged_blocks`
- `copy_skip_unchanged_blocks_min_blocks`
- `metadata_cache_ttl_seconds`
- `statfs_cache_ttl_seconds`
- `synchronous_commit`

### Narzędzie do tworzenia schematu

`mkfs.dbfs.py` obsługuje:

`init` jest idempotentne i nie usuwa `public`; `upgrade` odtwarza brakujące obiekty DBFS i przywraca `schema_version`; `clean` to jedyna destrukcyjna operacja narzędzia schematu, a po usunięciu publicznego schematu DBFS staje się no-opem. Narzędzie schematu używa jednego jawnego źródła hasła administracyjnego schematu: `--schema-admin-password`. Jeśli hasła brakuje, `init`, `upgrade` i `clean` kończą się natychmiast, bez promptu i bez ukrytej generacji sekretu. `mkfs.dbfs.py status` pokazuje tylko, czy sekret administracyjny schematu jest obecny i czy DBFS jest gotowy, bez ujawniania samego sekretu.

| Parametr | Typ | Domyślnie | Efekt |
| --- | --- | --- | --- |
| `init` | akcja | wymagane | Tworzy lub naprawia schemat DBFS bez usuwania obcych obiektów. |
| `upgrade` | akcja | wymagane | Odtwarza brakujące obiekty DBFS i przywraca `schema_version` do wersji kodu. |
| `clean` | akcja | wymagane | Usuwa obiekty DBFS utworzone przez narzędzie schematu. |
| `--block-size N` | CLI | `4096` | Ustawia domyślny rozmiar bloku używany przy inicjalizacji schematu. |
| `--schema-admin-password PASS` | CLI | generowane przy pierwszym `init`/`upgrade` | Sekret narzędzia schematu zapisany w bazie i wymagany przy późniejszych wywołaniach `init` / `upgrade` / `clean` na istniejącej bazie. |
| `--generate-client-tls-pair 1` | CLI | wyłączone | Generuje lokalną parę certyfikat/klucz TLS PostgreSQL podczas `init` lub `upgrade`. Użyj `0`, żeby wyłączyć jawnie. |
| `--tls-material-dir PATH` | CLI | `.dbfs/tls` | Ustawia katalog dla wygenerowanych materiałów TLS PostgreSQL. |
| `--tls-common-name NAME` | CLI | `dbfs` | Ustawia common name dla wygenerowanych materiałów TLS. |
| `--tls-cert-days N` | CLI | `365` | Ustawia czas ważności wygenerowanych materiałów TLS. |

## Docker Lab

Dla lokalnego backendu PostgreSQL:

```bash
make up
make init
make smoke
make mount
# w drugim terminalu:
make unmount

# demo w jednym kroku:
make demo

# test integracyjny:
make test-integration

# autodetekcja roli:
make test-role-autodetect

# pełny lokalny check:
make test-all

# rozszerzony pełny lokalny check:
make test-all-full
```

Osobne targety są rozdzielone tak, żeby można było odpalać tylko interesujący obszar:

- `make test-files`
- `make test-block-read`
- `make test-directories`
- `make test-metadata`
- `make test-symlink`
- `make test-destroy`
- `make test-locking`
- `make test-permissions`
- `make test-hardlink`
- `make test-fallocate`
- `make test-copy-file-range`
- `make test-ioctl`
- `make test-mknod`
- `make test-bufio`
- `make test-lseek`
- `make test-poll`
- `make test-utimens-noop`
- `make test-timestamp-touch-once`
- `make test-read-ahead-sequence`
- `make test-read-cache-benchmark`
- `make test-runtime-config`
- `make test-runtime-validation`
- `make test-mkfs-pg-tls`
- `make test-metadata-cache`
- `make test-runtime-profile`
- `make test-schema-upgrade`
- `make test-schema-status`
- `make test-access-groups`
- `make test-inode-model`
- `make test-ownership-inheritance`
- `make test-bmap`
- `make test-statfs-use-ino`
- `make test-atime-noatime`
- `make test-atime-relatime`
- `make test-pool-connections`
- `make test-mount-suite`
- `make test-all-full`

## Helper montowania

Jeśli chcesz, żeby DBFS działał jak helper `mount.dbfs`, zainstaluj skrypt do katalogu z `PATH`:

```bash
sudo install -m 755 mount.dbfs /usr/local/sbin/mount.dbfs
```

To samo możesz zrobić przez:

```bash
make install-mount-helper
```

Potem możesz montować DBFS tak:

```bash
mount.dbfs /mnt/dbfs
```

Opcje specyficzne dla DBFS możesz przekazać przez `-o`, na przykład:

```bash
mount.dbfs /mnt/dbfs -o role=auto,selinux=off,acl=off,default_permissions
```

Jeśli potrzebujesz własnego pliku konfiguracyjnego, ustaw `DBFS_CONFIG` przed uruchomieniem helpera:

```bash
DBFS_CONFIG=/ścieżka/do/dbfs_config.ini mount.dbfs /mnt/dbfs
```

Co sprawdzają testy:

- `make test-files` sprawdza create/write/truncate/rename/unlink.
- `make test-directories` sprawdza mkdir/rmdir/rename/stat/ls na drzewach katalogów oraz potwierdza, że `unlink()` na katalogu kończy się `EPERM`.
- `make test-metadata` sprawdza stat, chmod, chown, read, write, touch, truncate, access, stabilne raportowanie `st_dev` oraz aktualizacje `ctime`/`mtime`/`atime` przy zmianach metadanych, w tym jawne semantyki `touch -a` i `touch -m` oraz no-op `truncate` dla niezmienionego rozmiaru.
- `make test-write-noop` sprawdza, że zero-length `write()` jest no-op i nie podbija `ctime`, `mtime` ani rozmiaru pliku.
- `make test-symlink` sprawdza `ln -s`, `readlink`, `cat` przez symlink, `mv` na samym symlinku oraz przypadek osieroconego symlinka po usunięciu targetu. Test pokazuje też uszkodzony link przez `ls -al` na samej ścieżce symlinka.
- `make test-destroy` sprawdza, że `destroy()` flushuje bufory i zostawia dane trwałe dla nowej instancji DBFS.
- `make test-journal` sprawdza, że journal zapisuje główne operacje mutujące w kolejności i przechowuje aktualny uid procesu.
- `make test-locking` sprawdza semantykę locków i zachowanie własności, w tym konflikty zakresów, współistnienie shared locków i czyszczenie po unlock.
- `make test-pg-lock-manager` sprawdza produkcyjny backend locków oparty o PostgreSQL z TTL i heartbeat, w tym regresję dla dwóch klientów piszących do tego samego pliku.
- `make test-permissions` sprawdza egzekwowanie sticky bit przy `unlink`/`rmdir`, odrzucanie `chmod` na symlinkach, root-only `chown` na symlinkach, sprawdzanie właściciela/roota plus `chown` z uwzględnieniem grup dodatkowych, traktowanie `chown(-1, -1)` jako no-op, traktowanie `chown` z niezmienioną własnością jako no-op zarówno na plikach, jak i katalogach, traktowanie `chmod` z niezmienionym trybem jako no-op zarówno na plikach, jak i katalogach, zdejmowanie `setuid`/`setgid` przy zmianie własności zwykłych plików oraz zachowanie `setgid` na katalogach przy jednoczesnym zdejmowaniu `setuid` po zmianie własności.
- `make test-utimens-noop` sprawdza, że `utimens` z niezmienionymi timestampami jest no-op i nie podbija `ctime` zarówno na zwykłych plikach, jak i katalogach.
- Uwagi zgodności z `pjdfstest`: DBFS zostawia `unlink()` na katalogach jako `EPERM`, zachowuje bit `setgid` katalogów przy zmianach własności i traktuje przypadki brzegowe `utimens` oraz zmian własności zgodnie z zachowaniem Linux/POSIX widocznym w tym zestawie testów.
- `make test-hardlink` sprawdza tworzenie hardlinków, rename i zachowanie link count przez backend.
- `make test-fallocate` sprawdza preallocation i wzrost wypełniony zerami przez backend.
- `make test-copy-file-range` sprawdza kopiowanie danych z offsetami przez backend.
- `make test-ioctl` sprawdza wsparcie `FIONREAD` przez backend.
- `make test-mknod` sprawdza tworzenie FIFO i char-device oraz raportowanie `stat` typu i `rdev`. `open` dla special node'ów nadal jest unsupported.
- `make test-bufio` sprawdza backendowe `read_buf`/`write_buf` i trzyma ich semantykę w zgodzie z `read`/`write`.
- `make test-lseek` sprawdza backendowy seek helper dla `SEEK_SET`, `SEEK_CUR` i `SEEK_END`.
- `make test-poll` sprawdza backendowy poll helper dla plików regularnych.
- `make test-access-groups` sprawdza `access()` dla właściciela, grupy podstawowej i grup dodatkowych.
- `make test-inode-model` sprawdza, że `st_ino` przeżywa rename i restart DBFS dla katalogów, plików, hardlinków i symlinków.
- Model inode używa trwałych `inode_seed`, a hot-path query są oparte o `UNION ALL` oraz indeksy na `hardlinks.id_file` i `data_blocks(id_file, _order)`.
- `make test-ownership-inheritance` sprawdza, że `chmod`/`chown` na katalogu z `setgid` powoduje dziedziczenie `gid` przez nowe dzieci, a `rename` zachowuje metadane źródła i `mkdir` propaguje `setgid` do nowych podkatalogów.
- `make test-rename-root-conflict` sprawdza replace semantics dla plików i katalogów oraz edge-case'y dla `rename` na root.
- `make test-bmap` sprawdza logical block mapping dla regularnych plików i hardlinków. To nie jest fizyczny map bloków, tylko najbardziej stabilne mapowanie dostępne w filesystemie opartym o PostgreSQL.
- `make test-statfs-use-ino` sprawdza, przez mały shell smoke, że inode widoczne na mountcie zgadzają się z backendem, a `statvfs()` zwraca te same wartości filesystemowe co backendowy helper `statfs()`.
- `make test-mount-root-permissions` sprawdza świeży mount root oraz zachowanie chmod/chown/write dla katalogu na nowo zamontowanym filesystemie.
- `make test-atime-noatime` sprawdza zachowanie `atime` DBFS w trybie `noatime` i potwierdza, że odczyt nie podnosi `atime`.
- `make test-atime-relatime` sprawdza zachowanie `atime` DBFS w trybie `relatime` i potwierdza, że stary `atime` aktualizuje się po odczycie.
- `make test-atime-benchmark` wypisuje krótki baseline wall-time dla zachowania `atime` DBFS na odczytach plików i listowaniu katalogów, żeby porównać uruchomienia `default`, `noatime` i `nodiratime` bez długiej pętli smoke.
- `make test-pool-connections` sprawdza, że DBFS startuje pulę PostgreSQL z ustawionym limitem połączeń.
- `make test-mount-suite` to główny Pythonowy mount smoke suite; obejmuje pliki, katalogi, metadane, access modes, symlinki, `ioctl/FIONREAD`, `read`-driven `atime` dla plików, runtime-off dla ACL/SELinux, SELinux-on gdy jest włączony, `df` i tryb read-only dla repliki.
- `make test-throughput` uruchamia prosty benchmark `dd if=/dev/zero` na zamontowanym DBFS i wypisuje czas oraz MiB/s.
- `make test-throughput-sync` to wariant z `conv=fsync`.
- `make test-large-copy-benchmark` mierzy duży transfer `copy_file_range()` przez backend i wypisuje czas oraz MiB/s.
- `make test-large-file-multiblock-benchmark` mierzy duży zapis wieloblokowego pliku i wypisuje czasy write/persist/flush.
- `make test-remount-durability-benchmark` sprawdza, że dane przeżywają cykl stop/remount/reopen i wypisuje czas round-trip.
- `make test-tree-scale` benchmarkuje `getattr` i `readdir` na większym, zasilonym drzewie i pokazuje czasy `ls`/`find`.
- `make test-flush-release-profile` sprawdza, że czyste `flush()` / `release()` są tanie, a dirty flush persystuje dane dokładnie raz.
- `make test-write-flush-threshold` sprawdza, że niski próg auto-flush potrafi wypchnąć dirty dane przed zamknięciem i że bufor nie zostaje dirty po zapisie.
- `make test-all-full` rozszerza `make test-all` o workflow dla files/directories/metadata/symlink, shellowy smoke `statfs/use_ino`, mount workflow, oba smoke profile `atime` i benchmark throughput.

`make test-all` zawiera check xattr/SELinux/trusted/ACL oraz złożony mount smoke suite.
Mount repliki można wymusić przez `--role replica`. Domyślne `--role auto` wykrywa replikę przez `pg_is_in_recovery()` i montuje filesystem jako read-only.

Aktualne baseline'y porównawcze dla throughput, dużego copy, dużych wieloblokowych plików, durability po remount, read cache i zachowania `atime` są zapisane w [BENCHMARKS.md](BENCHMARKS.md).

## Opcje runtime

Jeśli potrzebujesz `allow_other`, uruchom mount z `DBFS_ALLOW_OTHER=1`, ale tylko wtedy, gdy `/etc/fuse.conf` na to pozwala.
W `/etc/dbfs/dbfs_config.ini` można też dodać sekcję `[dbfs]` z `pool_max_connections = N`, żeby ograniczyć liczbę połączeń PostgreSQL, które może otworzyć pula DBFS. Ta sama sekcja może także ustawiać domyślne parametry storage/read, takie jak `write_flush_threshold_bytes`, `read_cache_blocks`, `read_ahead_blocks`, `sequential_read_ahead_blocks`, `small_file_read_threshold_blocks`, `metadata_cache_ttl_seconds` i `statfs_cache_ttl_seconds`. Jeśli tego pliku nie ma, DBFS użyje `dbfs_config.ini` z katalogu projektu.
Ta sama sekcja może też ustawiać parametry wielowątkowości dla większych odczytów i kopiowania, takie jak `workers_read`, `workers_read_min_blocks`, `workers_write` i `workers_write_min_blocks`, oraz `persist_buffer_chunk_blocks`, które decyduje o wielkości paczek `execute_values()` podczas flushu. `workers_read` jest używane tylko wtedy, gdy brakujące bloki w odczycie dzielą się na kilka rozłącznych zakresów, a `workers_write` tylko wtedy, gdy kopiowanie można podzielić na kilka segmentów źródłowych. `block_size` nadal ma znaczenie, bo heurystyki workerów działają na blokach, a nie na surowych bajtach, więc mniejszy albo większy blok zmienia moment, w którym wielowątkowość zaczyna mieć sens, ale nie oznacza automatycznie "4 KiB = jeden wątek". Dla powtarzanych kopii typu rsync można też włączyć `copy_skip_unchanged_blocks`, żeby porównywać bloki docelowe i pomijać niezmienione zakresy podczas `copy_file_range()`; domyślnie jest to wyłączone, żeby nie spowalniać zwykłych kopii. Może też ustawiać `synchronous_commit`, żeby sterować trwałością sesji PostgreSQL dla każdego połączenia; dozwolone wartości to `on`, `off`, `local`, `remote_write` i `remote_apply`.
Jeśli chcesz gotowy preset produkcyjny, ustaw `DBFS_PROFILE=bulk_write`, `DBFS_PROFILE=metadata_heavy` albo `DBFS_PROFILE=pg_locking` przed mountem. Wybrany profil nadpisuje bazowe wartości z `[dbfs]` w `dbfs_config.ini`.
Profil możesz też podać jawnie jako `--profile bulk_write` do `dbfs_bootstrap.py` / `dbfs-bootstrap` albo jako `-o profile=bulk_write` do `mount.dbfs`.
Ta sama zmienna `DBFS_PROFILE` działa też z `make mount`, `make mount-user` i `make demo`.

Wsparcie xattr dla SELinux jest sterowane przez `--selinux auto|on|off` albo `DBFS_SELINUX=auto|on|off`.
Domyślnie jest `off`. `on` wymusza aktywację, a `auto` używa wykrywania po stronie hosta.
Wsparcie POSIX ACL jest sterowane przez `--acl on|off` albo `DBFS_ACL=on|off`.
Domyślnie jest `off`.
Przy starcie DBFS loguje efektywny profil runtime, wersję schematu, ustawienia TLS PostgreSQL, trwałość sesji PostgreSQL (`synchronous_commit`), tuning storage, opcje mounta i backend locków, więc można łatwo sprawdzić, jakie wartości faktycznie zostały zastosowane.
`DBFS_WRITE_FLUSH_THRESHOLD_BYTES` steruje tym, ile dirty danych może się zebrać, zanim DBFS auto-persystuje duży bufor podczas `write()`, `truncate()`, `fallocate()` albo `copy_file_range()`. Domyślna wartość to `67108864` bajtów.
`metadata_cache_ttl_seconds` steruje krótkim cache TTL dla odczytów metadanych `getattr()` i `readdir()`. Domyślna wartość to `1` sekunda.
`statfs_cache_ttl_seconds` steruje krótkim cache TTL dla `statfs()`. Domyślna wartość to `2` sekundy.
`DBFS_METADATA_CACHE_TTL_SECONDS` i `DBFS_STATFS_CACHE_TTL_SECONDS` nadpisują odpowiednie wartości z `dbfs_config.ini`, jeśli chcesz stroić te cache per środowisko.
`DBFS_PROFILE` wybiera nazwany profil runtime z `dbfs_config.ini`, na przykład `bulk_write` albo `metadata_heavy`.
`DBFS_ATIME_POLICY` jest wewnętrznym przełącznikiem DBFS, a nie surową opcją mounta FUSE. Steruje tym, kiedy DBFS aktualizuje `atime` w swoim własnym read path; `noatime`, `nodiratime`, `relatime` i `strictatime` są obsługiwane wewnętrznie i nie są przekazywane do `fusepy`.
Dla jednego uchwytu DBFS zapisuje `access_date` tylko raz, aby nie przepisywać ciągle tego samego rekordu podczas pojedynczej sekwencji open/read lub open/readdir. Kolejne dotknięcia są pomijane aż do zwolnienia uchwytu.
Ten sam model dotyczy też zapisu `mtime`/`ctime`: wiele zapisów na tym samym otwartym pliku aktualizuje te znaczniki dopiero przy persystencji dirty bufora, a nie przy każdym pośrednim wywołaniu `write()`.
Cache odczytu domyślnie ma większy blokowy LRU, a sekwencyjne odczyty automatycznie zwiększają read-ahead, dzięki czemu sąsiednie odczyty częściej trafiają w prefetche zamiast ponownie walić w PostgreSQL.

## Backup i restore

Backup i restore DBFS to w praktyce backup i restore PostgreSQL.

1. Użyj `pg_dump` / `pg_dumpall` albo standardowych narzędzi backupu PostgreSQL.
1. Odtwarzaj do instancji PostgreSQL zgodnej z wersją schematu DBFS.
1. Po restore możesz uruchomić `make test-schema-upgrade`, żeby szybko sprawdzić bezpieczeństwo `init` i naprawę wersji schematu.
1. Trzymaj dump bazy i użyty profil `dbfs_config.ini` razem, żeby restore wrócił do tego samego baseline strojenia.

Opcje widoczne w mount:

- `--default-permissions` jest włączone domyślnie; wyłącz przez `--no-default-permissions`, jeśli chcesz tylko checks FUSE.
- Zachowanie `atime` DBFS można wybrać przez `--atime-policy default|noatime|nodiratime|relatime|strictatime`.
- `noatime` wyłącza aktualizację `atime` dla odczytów plików i listowania katalogów; `nodiratime` wyłącza aktualizację `atime` katalogów, ale zostawia aktualizację `atime` plików.
- Dostępne są też `--lazytime`, `--sync` i `--dirsync`.
- Label SELinux można podać przez `DBFS_SELINUX_CONTEXT`, `DBFS_SELINUX_FSCONTEXT`, `DBFS_SELINUX_DEFCONTEXT` i `DBFS_SELINUX_ROOTCONTEXT`.
- Ustaw `DBFS_LOG_LEVEL=DEBUG`, jeśli chcesz pełne diagnostyczne tracebacki; domyślnie jest `INFO`, więc oczekiwane przypadki `ENODATA` nie będą zaśmiecały logów.
- `--acl on` jest wymagane, jeśli chcesz egzekwować ACL podczas runtime; inaczej xattr ACL pozostają nieaktywne.
- `--selinux on` lub `--selinux auto` jest wymagane, jeśli chcesz, żeby `security.selinux` było aktywne podczas runtime; inaczej xattr SELinux pozostają nieaktywne.
- `make test-mount-suite` zawiera zarówno smoke dla SELinux-off, jak i SELinux-on; przypadek SELinux-on jest pomijany automatycznie, jeśli mount nie startuje z `DBFS_SELINUX=on|auto`.
- DBFS przechowuje etykiety SELinux jako xattr i steruje nimi w runtime; nie implementuje samodzielnie pełnej polityki label mount.
- To zachowanie jest celowe: w tym repo pełna polityka mount-label jest poza zakresem, a zachowanie SELinux opiera się na host policy plus przechowywaniu xattr.
- `mknod` tworzy FIFO i char device metadata; `st_rdev` i `st_dev` są raportowane, ale `open` dla special node'ów nadal jest unsupported.
- `system.posix_acl_*` działa dla access ACL i default ACL inheritance; backend zapisuje, propaguje i egzekwuje ACL.
- `poll` działa jako backend helper dla zwykłych plików; natywny hook FUSE nadal zależy od możliwości `fusepy`.

## Troubleshooting

- Zacznij od `mkfs.dbfs.py status`, żeby zobaczyć, czy sekret administracyjny schematu jest obecny i czy DBFS jest gotowy.
- Jeśli `mkfs.dbfs.py init` kończy się błędem, sprawdź czy PostgreSQL działa i czy dane w `dbfs_config.ini` zgadzają się z serwerem.
- Jeśli montowanie kończy się `DBFS schema is not initialized`, uruchom najpierw `make init`; dla operacji `mkfs.dbfs.py` zawsze podawaj `--schema-admin-password`.
- Jeśli montowanie kończy się `DBFS schema version mismatch`, uruchom `mkfs.dbfs.py upgrade` z sekretem administracyjnym schematu, żeby wersja schematu zgadzała się z kodem.
- Przy udanym starcie mounta DBFS loguje `DBFS schema version=<db> expected=<code>`, więc możesz od razu potwierdzić zgodność wersji przed użyciem mounta.
- Jeśli montowanie kończy się `ENOTCONN` albo błędem połączenia, uruchom najpierw `make smoke`, żeby potwierdzić łączność z bazą.
- Jeśli brakuje `fusermount3`, spróbuj `fusermount` albo doinstaluj narzędzia userspace FUSE dla swojej dystrybucji.
- Jeśli `allow_other` jest ignorowane, sprawdź `/etc/fuse.conf` i upewnij się, że `user_allow_other` jest włączone.
- Jeśli ACL albo SELinux wyglądają na nieaktywne, upewnij się, że mount został uruchomiony z `--acl on` albo `--selinux on|auto`.

## Rekomendowane profile mounta

| Profil | Zastosowanie | Kluczowe opcje |
| --- | --- | --- |
| `dbfs-relaxed` | Lokalny dev i smoke testy | `--no-default-permissions`, `DBFS_ACL=off`, `DBFS_SELINUX=off`, `--atime-policy default` |
| `dbfs-linux-default` | Najbliżej typowego mounta Linuksa | `--default-permissions`, `DBFS_ACL=off`, `DBFS_SELINUX=off`, `--atime-policy relatime` |
| `dbfs-selinux` | Środowiska z SELinux | `--default-permissions`, `DBFS_ACL=on`, `DBFS_SELINUX=auto` albo `on`, `DBFS_SELINUX_CONTEXT` według potrzeb |

## Rekomendowane workloady

| Profil runtime | Dobry dla | Dlaczego |
| --- | --- | --- |
| `dbfs-relaxed` | Lokalny development, smoke runy i szybkie testy ręczne | Najmniej restrykcyjna polityka i najluźniejsza semantyka mounta. |
| `dbfs-linux-default` | Mieszane workloady z zachowaniem zbliżonym do typowego mounta Linuksa | Zbalansowane ustawienia dla ACL-off, SELinux-off i zachowania podobnego do relatime. |
| `bulk_write` | Duży ingest sekwencyjny, `copy_file_range()`, testy throughputu, durability po remount | Większe batchowanie flush i bardziej agresywne strojenie strony zapisu. |
| `metadata_heavy` | `ls`, `find`, `stat`, przeglądanie głębokich drzew, operacje tylko na metadanych | Dłuższy TTL cache metadanych i bardziej zachowawcza presja na write path. |
| `pg_locking` | Koordynacja wielu klientów i testy regresji locków | Strojenie backendu locków z krótszym poll interval do sprawdzania lease'ów. |

## Antywzorce

- Nie używaj `bulk_write` do nawigacji po metadanych albo pracy na wielu małych plikach; ten profil jest pod throughput, nie pod niską latencję namespace.
- Nie używaj `metadata_heavy` do dużego sekwencyjnego ingestu albo `copy_file_range()`; ten profil jest świadomie bardziej zachowawczy po stronie zapisu.
- Nie używaj `dbfs-relaxed` dla wieloużytkowych albo produkcyjnych mountów, gdzie potrzebujesz bardziej linuksowej semantyki uprawnień.
- Nie traktuj `synchronous_commit=off` jako domyślnego ustawienia trwałości; stosuj je tylko wtedy, gdy workload akceptuje kompromis i benchmark pokazuje sens.
- Nie oczekuj, że `pg_locking` sam poprawi throughput zapisu; ten profil dotyczy koordynacji i semantyki, a nie przyspieszania data path.

## Docelowa Architektura

DBFS pozostaje teraz celowo jako Pythonowy frontend FUSE:

- Python odpowiada za bootstrap, `mkfs`, ładowanie configów i profili, callbacki FUSE, logikę administracyjną, migracje schematu, testy integracyjne oraz warstwy polityk typu ACL/permissions/journal/runtime validation.
- Rust jest najbardziej prawdopodobnym długoterminowym silnikiem hot-path dla składania bloków, write overlay, segmentacji copy i przygotowania persist, jeśli i kiedy ten kod zostanie wyjęty z Pythona.
- Cel to cieńszy `dbfs_fuse.py`, więcej delegacji do osobnych modułów i przyszły natywny core tylko tam, gdzie benchmarki pokażą realny zysk.
