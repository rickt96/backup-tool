"""Backup incrementale delle cartelle di un telefono verso il PC.

Uso:   python backup.py <nome-telefono>              # nome = blocco [phones.<nome>] in config.toml
       python backup.py <nome-telefono> --dry-run   # mostra cosa farebbe, senza scrivere ne' cancellare
       python backup.py --selftest                  # check della logica pura

Incrementale: un file viene scaricato solo se manca a destinazione o ha
dimensione diversa. I media del telefono sono immutabili, quindi basta.

Ogni voce di `dirs` in config puo' essere una stringa (mode "copy", default)
oppure {path = "...", mode = "move"}: in "move" il file viene cancellato dal
telefono dopo che la copia locale e' verificata (dimensione uguale).
"""

import concurrent.futures as cf
import ftplib
import os
import sys
import threading
from datetime import datetime, timedelta
from pathlib import Path

try:
    import tomllib  # stdlib da Python 3.11
except ModuleNotFoundError:  # pragma: no cover
    sys.exit("Serve Python 3.11+ (oppure: pip install tomli e cambia l'import)")

CONFIG = Path(__file__).with_name("config.toml")


# --- logica pura (testabile senza rete) --------------------------------------

def local_path(out_root: Path, root: str, remote: str) -> Path:
    """Percorso locale corrispondente a un file remoto, sotto out_root."""
    rel = remote[len(root) + 1:] if root else remote.lstrip("/")
    return out_root / rel


def parse_dirs(entries: list) -> list[tuple[str, str]]:
    """Ogni voce di `dirs`: "path" (copy) oppure {path=..., mode="move"}.

    mode "move" = scarica e poi cancella dal telefono (solo a copia verificata).
    """
    out = []
    for e in entries:
        if isinstance(e, str):
            out.append((e, "copy"))
            continue
        mode = e.get("mode", "copy")
        if mode not in ("copy", "move"):
            sys.exit(f"mode '{mode}' non valido (copy | move) in dirs")
        out.append((e["path"], mode))
    return out


def needs_download(local: Path, remote_size: int) -> bool:
    if remote_size < 0:
        return True  # dimensione ignota dal server: scarica per sicurezza
    return not local.exists() or local.stat().st_size != remote_size


_MONTHS = {m: i for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1)}


def parse_list_line(line: str):
    """Riga di `LIST` unix (ls -l) -> (name, size, mtime_ts, is_dir), o None.

    ponytail: gestisce solo il formato unix; i server FTP Android lo usano tutti.
    Aggiungere il formato DOS solo se serve davvero.
    """
    parts = line.split(None, 8)
    if len(parts) < 9 or parts[0][0] not in "d-l":
        return None
    perms, _, _, _, size, mon, day, tm, name = parts
    if perms[0] == "l":  # "nome -> target"
        name = name.split(" -> ", 1)[0]
    is_dir = perms[0] == "d"
    return name, (0 if is_dir else int(size)), parse_list_time(mon, day, tm), is_dir


def parse_list_time(mon: str, day: str, tm: str) -> float | None:
    """"Jul 21 23:37" o "Jul 21 2024" (ora locale del telefono) -> epoch."""
    try:
        month = _MONTHS[mon]
        now = datetime.now()
        if ":" in tm:
            hh, mm = map(int, tm.split(":"))
            dt = datetime(now.year, month, int(day), hh, mm)
            if dt > now + timedelta(days=1):  # data futura => è dell'anno scorso
                dt = dt.replace(year=now.year - 1)
        else:
            dt = datetime(int(tm), month, int(day))
        return dt.timestamp()
    except (KeyError, ValueError):
        return None


# --- trasporto FTP ----------------------------------------------------------

def ftp_connect(p: dict) -> ftplib.FTP:
    ftp = ftplib.FTP()
    ftp.encoding = p.get("encoding", "utf-8")  # se i nomi file crashano: "latin-1"
    ftp.connect(p["host"], int(p.get("port", 21)), timeout=30)
    ftp.login(p.get("user", "anonymous"), p.get("password", ""))
    ftp.voidcmd("TYPE I")  # download binari + LIST coerente
    return ftp


def _abspath(d: str) -> str:
    return "/" + d.strip("/") if d.strip("/") else "/"


def ftp_cwd(ftp: ftplib.FTP, d: str) -> bool:
    """CWD assoluto, ricordando l'ultima posizione per saltare i CWD ridondanti."""
    target = _abspath(d)
    if getattr(ftp, "_cwd", None) == target:
        return True
    try:
        ftp.cwd(target)
        ftp._cwd = target
        return True
    except ftplib.error_perm as e:
        print(f"  ! salto {d}: {e}")
        return False


def _list_dir(ftp: ftplib.FTP, d: str):
    """Yield (path, size, mtime_ts, is_dir) del contenuto diretto di d via LIST."""
    if not ftp_cwd(ftp, d):
        return
    lines: list[str] = []
    ftp.retrlines("LIST", lines.append)
    for line in lines:
        parsed = parse_list_line(line)
        if parsed is None:
            continue
        name, size, mtime_ts, is_dir = parsed
        if name in (".", ".."):
            continue
        yield f"{_abspath(d)}/{name}", size, mtime_ts, is_dir


def ftp_walk(ftp: ftplib.FTP, root: str):
    """Yield (remote_path, size, mtime_ts) per ogni file sotto root, ricorsivo."""
    stack = [root]
    while stack:
        d = stack.pop()
        for path, size, mtime_ts, is_dir in _list_dir(ftp, d):
            if is_dir:
                stack.append(path)
            else:
                yield path, size, mtime_ts


_tl = threading.local()


def _worker_ftp(p: dict) -> ftplib.FTP:
    ftp = getattr(_tl, "ftp", None)
    if ftp is None:
        ftp = _tl.ftp = ftp_connect(p)
    return ftp
    # ponytail: le connessioni per-thread si chiudono da sole a fine processo.
    # Aggiungere un pool con cleanup solo se lo script diventa long-running.


def ftp_download(p: dict, remote: str, local: Path, size: int, mtime_ts: float | None):
    local.parent.mkdir(parents=True, exist_ok=True)
    tmp = local.with_suffix(local.suffix + ".part")
    ftp = _worker_ftp(p)
    d, base = remote.rsplit("/", 1)
    ftp_cwd(ftp, d)  # SwiFTP & co. vogliono CWD + RETR <nome>, non RETR <path>
    with open(tmp, "wb") as f:
        ftp.retrbinary(f"RETR {base}", f.write)
    got = tmp.stat().st_size
    if size >= 0 and got != size:  # scaricato parziale: NON promuovere (e NON cancellare dal telefono)
        tmp.unlink()
        raise OSError(f"download incompleto: {got}/{size} byte")
    tmp.replace(local)
    if mtime_ts:
        os.utime(local, (mtime_ts, mtime_ts))


def ftp_delete(ftp: ftplib.FTP, remote: str):
    d, base = remote.rsplit("/", 1)
    ftp_cwd(ftp, d)
    ftp.delete(base)


# --- trasporto locale (telefono montato come E:\, share SMB, ...) -----------

def local_walk(root: str):
    base = Path(root)
    for f in base.rglob("*"):
        if f.is_file():
            st = f.stat()
            yield f.as_posix(), st.st_size, st.st_mtime


def local_copy(_p, remote: str, local: Path, size: int, mtime_ts: float | None):
    local.parent.mkdir(parents=True, exist_ok=True)
    data = Path(remote).read_bytes()
    if size >= 0 and len(data) != size:
        raise OSError(f"copia incompleta: {len(data)}/{size} byte")
    local.write_bytes(data)
    if mtime_ts:
        os.utime(local, (mtime_ts, mtime_ts))


def local_delete(_conn, remote: str):
    Path(remote).unlink()


# --- orchestrazione --------------------------------------------------------

def run(name: str, dry_run: bool = False):
    cfg = tomllib.loads(CONFIG.read_text(encoding="utf-8"))
    phones = cfg.get("phones", {})
    if name not in phones:
        sys.exit(f"telefono '{name}' assente. Disponibili: {', '.join(phones) or '(nessuno)'}")
    p = phones[name]
    out_root = Path(cfg["dest"]) / name
    root = str(p.get("root", "")).rstrip("/")
    is_ftp = p["transport"] == "ftp"

    if is_ftp:
        lister = ftp_connect(p)
        walk = lambda d: ftp_walk(lister, d)
        transfer, remote_delete = ftp_download, ftp_delete
    elif p["transport"] == "local":
        lister = None
        walk = local_walk
        transfer, remote_delete = local_copy, local_delete
    else:
        sys.exit(f"transport '{p['transport']}' non supportato (ftp | local)")

    jobs, skipped = [], 0
    del_after_backup = []  # file in dir 'move' gia' salvati localmente -> da cancellare dal telefono
    for d, mode in parse_dirs(p["dirs"]):
        rdir = f"{root}/{d}" if root else d
        print(f"scan {rdir}  [{mode}]")
        for remote, size, mtime_ts in walk(rdir):
            lp = local_path(out_root, root, remote)
            if needs_download(lp, size):
                jobs.append((remote, lp, size, mtime_ts, mode))
            else:
                skipped += 1
                if mode == "move":
                    del_after_backup.append(remote)
    if lister:
        lister.quit()

    print(f"\n{len(jobs)} da copiare, {skipped} gia' presenti")
    if dry_run:
        for r, l, *_ , mode in jobs:
            print(f"  COPY {l.relative_to(out_root)}" + ("  + DEL dal telefono" if mode == "move" else ""))
        for r in del_after_backup:
            print(f"  DEL  {r}  (gia' salvato)")
        print("\n[dry-run] niente scaricato o cancellato")
        return True

    copied = failed = 0
    to_delete = list(del_after_backup)
    workers = int(p.get("workers", 4)) if is_ftp else 1
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(transfer, p, r, l, s, m): (r, l, mode)
                for r, l, s, m, mode in jobs}
        for fut in cf.as_completed(futs):
            r, l, mode = futs[fut]
            try:
                fut.result()
                copied += 1
                if mode == "move":
                    to_delete.append(r)
                print(f"  ok  {l.relative_to(out_root)}")
            except Exception as e:
                failed += 1
                print(f"  ERR {r}: {e}")

    deleted = 0
    if to_delete:
        print(f"\nrimuovo dal telefono {len(to_delete)} file (dir in modo 'move')")
        conn = ftp_connect(p) if is_ftp else None
        for r in to_delete:
            try:
                remote_delete(conn, r)
                deleted += 1
            except Exception as e:
                print(f"  ERR del {r}: {e}")
        if conn:
            conn.quit()

    print(f"\ncopiati {copied}, saltati {skipped}, falliti {failed}, rimossi dal telefono {deleted}")
    return failed == 0


def selftest():
    import tempfile
    with tempfile.TemporaryDirectory() as t:
        out = Path(t)
        assert local_path(out, "/storage/emulated/0", "/storage/emulated/0/DCIM/a.jpg") == out / "DCIM/a.jpg"
        assert local_path(out, "", "DCIM/a.jpg") == out / "DCIM/a.jpg"

        f = out / "x.jpg"
        assert needs_download(f, 10) is True            # manca
        f.write_bytes(b"0123456789")
        assert needs_download(f, 10) is False           # stessa dimensione
        assert needs_download(f, 99) is True            # dimensione diversa
        assert needs_download(f, -1) is True            # dimensione ignota

        d = parse_list_line("drwxr-xr-x 1 owner group 3452 Jul 21 23:37 downloaded_rom")
        assert d == ("downloaded_rom", 0, parse_list_time("Jul", "21", "23:37"), True)
        f2 = parse_list_line("-rw-r--r-- 1 owner group 275518 Jan 28 2024 menu di prova.pdf")
        assert f2[0] == "menu di prova.pdf" and f2[1] == 275518 and f2[3] is False
        assert parse_list_line("l--------- 1 o g 0 Jan 1 00:00 link -> /target")[0] == "link"
        assert parse_list_line("211 End") is None
        assert parse_list_time("Jan", "28", "2024") == datetime(2024, 1, 28).timestamp()

        assert parse_dirs(["A", {"path": "B", "mode": "move"}]) == [("A", "copy"), ("B", "move")]
        assert parse_dirs([{"path": "C"}]) == [("C", "copy")]
    print("selftest ok")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--dry-run"]
    dry = "--dry-run" in sys.argv[1:]
    if len(args) != 1:
        sys.exit(__doc__)
    if args[0] == "--selftest":
        selftest()
    else:
        sys.exit(0 if run(args[0], dry_run=dry) else 1)
