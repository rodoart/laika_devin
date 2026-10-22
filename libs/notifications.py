"""Reporte HTML único de corrida para ``main_dynamic``.

Un solo correo por fuente (éxito o error) con:

- cabecera: fuente, badge ``SUCCESS``/``FAILED``, ``year_id``, inicio/fin y
  duración total;
- tabla de etapas del ``steps_list`` en el orden ejecutado, con estado
  (``computed``/``reloaded``/``failed``/``pending``), duración propia de
  la etapa (``DatapullStageStep.action_duration``) y path de salida;
- conteo de clientes del pivot y del output final, número de columnas
  finales y desglose por transformación de fe ejecutada (las de
  ``config_custom["lista_trans"]``/``lista_trans_per_source``);
- en caso de error: la etapa fallada marcada y el traceback escapado en
  ``<pre>``.

Las funciones ``*_stats`` que leen parquets son defensivas: cualquier
fallo devuelve ``None`` para no romper el envío del correo.
"""
import html
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

#: Columnas que no cuentan como features en un parquet de salida.
_KEY_COLS = {"numcliente", "vintage_date", "id_msf_tools_v2_12315",
             "features_vec", "features"}

_STATUS_STYLE = {
    "computed": ("#1a7f37", "computed"),
    "reloaded": ("#0969da", "reloaded"),
    "failed": ("#cf222e", "FAILED"),
    "pending": ("#6e7781", "pending"),
}

_STYLE = """
<style type="text/css" media="screen">
  body { font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
         font-size: 13px; color: #222; }
  .hdr { background-color: #002072; color: white; padding: 12px 16px;
         border-radius: 4px; }
  .hdr h1 { margin: 0; font-size: 18px; }
  .hdr .sub { font-size: 12px; color: #cdd7ff; margin-top: 4px; }
  .badge { display: inline-block; padding: 2px 10px; border-radius: 10px;
           color: white; font-weight: bold; font-size: 12px; }
  table.rpt { border-collapse: collapse; margin: 12px 0; min-width: 520px; }
  table.rpt th { background-color: #002072; color: white; padding: 5px 10px;
                 border: 1px solid #ddd; text-align: left; }
  table.rpt td { border: 1px solid #ddd; padding: 5px 10px;
                 background-color: #f6f8fa; }
  table.rpt tr.failed td { background-color: #ffebe9; }
  .kv { margin: 12px 0; }
  .kv b { display: inline-block; min-width: 190px; }
  pre.tb { background: #f6f8fa; border: 1px solid #ddd; padding: 10px;
           font-family: Consolas, monospace; font-size: 11px;
           white-space: pre-wrap; word-wrap: break-word;
           max-height: 400px; overflow: auto; }
</style>
"""


def _h(x: Any) -> str:
    return html.escape(str(x))


def _fmt_dur(seconds: float) -> str:
    """``3724.1`` -> ``'1h 2m 4s'``."""
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{seconds}s"


def _fmt_ts(ts: Optional[datetime]) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "-"


def _n(x: Optional[int]) -> str:
    return f"{x:,}" if isinstance(x, int) else "-"


# ---------------------------------------------------------------------------
# lectura defensiva de stats (Spark)
# ---------------------------------------------------------------------------
def count_distinct_clients(sql_context: Any, path: str,
                           col_name: str = "numcliente") -> Optional[int]:
    """``count distinct`` de clientes en un parquet; ``None`` si falla."""
    try:
        df = sql_context.read.parquet(path)
        if col_name in df.columns:
            return df.select(col_name).distinct().count()
        return df.count()
    except Exception:
        return None


def parquet_num_columns(sql_context: Any, path: str) -> Optional[int]:
    """Nº de columnas de un parquet (solo schema, sin cómputo)."""
    try:
        return len(sql_context.read.parquet(path).columns)
    except Exception:
        return None


def feature_stats(sql_context: Any, features_dir: str,
                  trans_list: List[str]) -> Tuple[int, List[Tuple[str, int]]]:
    """Columnas de features por transformación ejecutada.

    Lee solo el schema de ``<features_dir>/<trans>_features`` para cada
    transformación de ``trans_list`` (las de ``config_custom``, no todas
    las posibles) y descuenta las columnas clave.

    Returns
    -------
    (total, detalle)
        ``total``: columnas de features sumadas; ``detalle``: lista
        ``(trans, n_cols)`` de las que escribieron salida.
    """
    total = 0
    detail: List[Tuple[str, int]] = []
    for trans in trans_list:
        n = parquet_num_columns(
            sql_context, os.path.join(features_dir, f"{trans}_features"))
        if n is None:
            continue
        try:
            cols = sql_context.read.parquet(
                os.path.join(features_dir, f"{trans}_features")).columns
            n = len([c for c in cols if c not in _KEY_COLS])
        except Exception:
            pass
        total += n
        detail.append((trans, n))
    return total, detail


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------
def _steps_table(chain: List[Any]) -> str:
    """Tabla etapa -> estado/duración/output para la cadena de steps."""
    rows = []
    for step in chain:
        status = getattr(step, "last_status", "pending")
        color, label = _STATUS_STYLE.get(status, _STATUS_STYLE["pending"])
        dur = getattr(step, "action_duration", 0.0)
        out = getattr(step, "output_path", "")
        write = getattr(step, "write_path", "")
        rel = os.path.relpath(out, write) if write and out.startswith(write) \
            else os.path.basename(out.rstrip("/"))
        cls = ' class="failed"' if status == "failed" else ""
        rows.append(
            f'<tr{cls}><td>{_h(step.stage)}</td>'
            f'<td><span class="badge" style="background:{color}">'
            f'{_h(label)}</span></td>'
            f'<td>{_fmt_dur(dur) if dur else "-"}</td>'
            f'<td>{_h(rel)}</td></tr>')
    return ('<table class="rpt"><tr><th>etapa</th><th>estado</th>'
            '<th>duración</th><th>salida</th></tr>' + "".join(rows) +
            "</table>")


def build_run_report(*, source: str, chain: List[Any], status: str,
                     started_at: datetime, finished_at: datetime,
                     config_custom: Dict[str, Any],
                     error_traceback: Optional[str] = None,
                     pivot_clients: Optional[int] = None,
                     final_clients: Optional[int] = None,
                     final_columns: Optional[int] = None,
                     feature_detail: Optional[List[Tuple[str, int]]] = None,
                     trans_list: Optional[List[str]] = None,
                     controls_html: Optional[str] = None,
                     ) -> Tuple[str, str]:
    """Construye ``(subject, html)`` del correo de fin de corrida.

    Parameters
    ----------
    source:
        Fuente ejecutada.
    chain:
        Steps de la corrida en orden (``iter_chain(last_step)``).
    status:
        ``"success"`` o ``"failed"``.
    error_traceback:
        Traceback formateado (solo si ``status == "failed"``).
    pivot_clients / final_clients / final_columns / feature_detail:
        Stats opcionales; ``None`` -> ``-``.
    trans_list:
        Transformaciones de fe configuradas (de ``config_custom``).
    controls_html:
        HTML de los "Input controls" del extract (input/output por
        tabla); se incrusta tal cual al final del reporte. ``None`` si
        el extract se recargó.
    """
    ok = status == "success"
    total_s = (finished_at - started_at).total_seconds()
    failed_step = next(
        (s.stage for s in chain
         if getattr(s, "last_status", "") == "failed"), None)

    total_feat_cols = (sum(n for _, n in feature_detail)
                       if feature_detail else None)
    if ok:
        subject = (f"[OK] Datapull {source} — {_fmt_dur(total_s)}"
                   + (f", {_n(total_feat_cols)} feat cols"
                      if isinstance(total_feat_cols, int) else ""))
    else:
        subject = (f"[FAILED] Datapull {source}"
                   + (f" — failed at {failed_step}" if failed_step else ""))

    badge = ('<span class="badge" style="background:#1a7f37">SUCCESS</span>'
             if ok else
             '<span class="badge" style="background:#cf222e">FAILED</span>')

    parts = [
        "<html><head><meta charset='utf-8'>" + _STYLE + "</head><body>",
        '<div class="hdr"><h1>Datapull — ' + _h(source) + " " + badge +
        "</h1>",
        (f'<div class="sub">year_id {_h(config_custom.get("year_id", "?"))}'
         f' · {_fmt_ts(started_at)} → {_fmt_ts(finished_at)}'
         f' · total {_fmt_dur(total_s)} · '
         f'n_mis={_h(config_custom.get("n_mis", "?"))}</div></div>'),

        "<h3>Etapas</h3>", _steps_table(chain),

        '<div class="kv">'
        f'<div><b>Clientes en pivot</b> {_n(pivot_clients)}</div>'
        f'<div><b>Clientes en salida final</b> {_n(final_clients)}</div>'
        + (f'<div><b>Columnas de features (total)</b> '
           f'{_n(total_feat_cols)}</div>' if total_feat_cols else "")
        + f'<div><b>Columnas en salida final</b> {_n(final_columns)}</div>'
        "</div>",
    ]

    if trans_list:
        parts.append("<h3>Features ejecutadas (config_custom)</h3>")
        rows = "".join(
            f'<tr><td>{_h(t)}</td><td>{_n(n)}</td></tr>'
            for t, n in (feature_detail or []))
        pendientes = set(trans_list) - {t for t, _ in (feature_detail or [])}
        rows += "".join(
            f'<tr><td>{_h(t)}</td><td><i>sin salida</i></td></tr>'
            for t in sorted(pendientes))
        parts.append('<table class="rpt"><tr><th>transformación</th>'
                     f'<th>cols</th></tr>{rows}</table>')

    if controls_html:
        parts.append("<h3>Controles del extract (input/output)</h3>")
        parts.append(controls_html)

    if error_traceback:
        parts.append("<h3>Error"
                     + (f" — etapa <b>{_h(failed_step)}</b>"
                        if failed_step else "") + "</h3>")
        parts.append(f'<pre class="tb">{_h(error_traceback)}</pre>')

    parts.append("</body></html>")
    return subject, "".join(parts)
