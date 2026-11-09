import itertools
from datetime import datetime, timedelta, timezone
from generador import eventos

T0 = datetime(2026, 11, 2, tzinfo=timezone(timedelta(hours=-3)))
SEMILLA = 2026
N_HIST, N_VIVO = 864_000, 7_200

def historico():
    return itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST)

def vivo():
    return itertools.islice(eventos(T0, semilla=SEMILLA), N_HIST, N_HIST + N_VIVO)