"""Tests de ``libs.email_sender.EmailSender`` y del HTML de los correos.

Todo es un *simulacro* — no se abre ninguna conexión SMTP:

- ``build_message`` construye el ``MIMEMultipart`` directamente.
- ``send(..., dry_run_dir=...)`` (o ``LAIKA_EMAIL_DRY_RUN_DIR``) escribe el
  correo completo en un ``.eml`` local en vez de enviarlo.
- El HTML de tiempos/controles se genera con las mismas funciones que usa
  el pipeline (``DataPull._create_html_notification_times``,
  ``Extract.create_html_controls``).
"""
import email
import os
from email import policy

import pandas as pd
import pytest

from libs.email_sender import EmailSender


def _html_body(msg):
    """Extrae el payload del part ``text/html`` decodificado."""
    return next(part.get_payload(decode=True).decode("utf-8")
                for part in msg.walk()
                if part.get_content_type() == "text/html")


def _parse_eml(path):
    with open(path, encoding="utf-8") as f:
        return email.message_from_string(f.read(), policy=policy.default)


class TestBuildMessage:
    def test_from_address(self):
        sender = EmailSender("ab12345")
        msg = sender.build_message(["cd67890"], "s", "<p>m</p>")
        assert msg["From"] == "ab12345@imcla.lac.nsroot.net"

    def test_to_multiple_receivers(self):
        sender = EmailSender("ab12345")
        msg = sender.build_message(["u1", "u2", "u3"], "s", "<p>m</p>")
        assert msg["To"] == ("u1@imcla.lac.nsroot.net, "
                             "u2@imcla.lac.nsroot.net, "
                             "u3@imcla.lac.nsroot.net")

    def test_subject_and_html_body(self):
        sender = EmailSender("s")
        msg = sender.build_message(["r"], "Asunto X", "<h1>Hola</h1>")
        assert msg["Subject"] == "Asunto X"
        assert _html_body(msg) == "<h1>Hola</h1>"

    def test_attachment_is_base64_with_filename(self, tmp_path):
        attachment = tmp_path / "datos.csv"
        attachment.write_bytes(b"col1,col2\n1,2\n")
        sender = EmailSender("s")
        msg = sender.build_message(["r"], "s", "<p>m</p>",
                                   file=str(attachment))
        parts = [p for p in msg.walk()
                 if p.get_content_type() == "application/octet-stream"]
        assert len(parts) == 1
        part = parts[0]
        assert part["Content-Disposition"] == \
            "attachment; filename= datos.csv"
        assert part.get_payload(decode=True) == b"col1,col2\n1,2\n"


class TestDryRunSend:
    def test_writes_eml_and_skips_smtp(self, tmp_path, monkeypatch):
        """El simulacro escribe el .eml y no toca SMTP."""
        def _boom(*a, **k):
            raise AssertionError("SMTP no debe llamarse en dry-run")
        monkeypatch.setattr("smtplib.SMTP", _boom)

        sender = EmailSender("ab12345")
        path = sender.send(["u1"], "Prueba local", "<p>cuerpo</p>",
                           dry_run_dir=str(tmp_path))

        assert path is not None and os.path.isfile(path)
        assert path.endswith(".eml")
        parsed = _parse_eml(path)
        assert parsed["From"] == "ab12345@imcla.lac.nsroot.net"
        assert parsed["To"] == "u1@imcla.lac.nsroot.net"
        assert parsed["Subject"] == "Prueba local"
        assert _html_body(parsed) == "<p>cuerpo</p>"

    def test_env_var_enables_dry_run(self, tmp_path, monkeypatch):
        monkeypatch.setattr("smtplib.SMTP", lambda *a, **k: pytest.fail(
            "SMTP llamado en dry-run"))
        monkeypatch.setenv("LAIKA_EMAIL_DRY_RUN_DIR", str(tmp_path))
        sender = EmailSender("s")
        path = sender.send(["u1"], "s", "<p>m</p>")
        assert path and os.path.isfile(path)

    def test_filename_is_safe(self, tmp_path, monkeypatch):
        monkeypatch.setattr("smtplib.SMTP", lambda *a, **k: pytest.fail(
            "SMTP llamado"))
        sender = EmailSender("s")
        path = sender.send(["u1"], "Error in execution of source pnl/x",
                           "<p>m</p>", dry_run_dir=str(tmp_path))
        assert "/" not in os.path.basename(path)


class TestNotificationEmails:
    """Construcción del HTML real que envía el pipeline + dry-run."""

    def test_times_email_html_and_draft(self, tmp_path, monkeypatch):
        """La tabla de tiempos llega completa al .eml."""
        monkeypatch.setattr("smtplib.SMTP", lambda *a, **k: pytest.fail(
            "SMTP llamado"))
        from datapull import DataPull

        times = pd.DataFrame({
            "steps": ["extract", "weekly_extract", "missing_treatment"],
            "time of execution": ["0h 1m 5s", "0h 0m 30s", "0h 2m 10s"],
        })
        dp = DataPull.__new__(DataPull)   # el método no usa estado
        html = dp._create_html_notification_times(
            times, DataPull.message_start_finals, DataPull.message_style,
            DataPull.message_end)

        # Estructura del HTML
        assert "<h1> Times of execution: </h1>" in html
        assert 'id="default"' in html        # tabla con el estilo #default
        assert "</body>" in html
        for stage in times["steps"]:
            assert stage in html

        # Simulacro de envío: el .eml contiene el mismo HTML
        sender = EmailSender("ab12345")
        path = sender.send(
            ["u1"], "Datapull process for source pnl completed", html,
            dry_run_dir=str(tmp_path))
        parsed = _parse_eml(path)
        assert parsed["Subject"] == "Datapull process for source pnl completed"
        body = _html_body(parsed)
        assert "Times of execution" in body
        assert "missing_treatment" in body

    def test_controls_email_html(self):
        """Extract.create_html_controls recorre las tablas con in/out."""
        from libs.extract import Extract

        ext = Extract.__new__(Extract)
        ext.tables_in = ["nomina_historic"]
        ext.controls_in = [pd.DataFrame(
            {"vintage_date": ["202307"], "tfrom": [1], "count": [56]})]
        ext.controls_out = [pd.DataFrame(
            {"vintage_date": ["202307"], "tfrom": [1], "count": [56]})]
        ext.controls_out_f = [pd.DataFrame(
            {"vintage_date": ["202307"], "tfrom": [1], "count": [54]})]

        from datapull import DataPull
        html = ext.create_html_controls(
            DataPull.message_start_controls_stract, DataPull.message_style,
            DataPull.message_end)

        assert "<h1> Input controls: </h1>" in html
        assert "Table: nomina_historic" in html
        assert html.count('id="default"') == 3   # in, out, out_f
        assert "Output (nulls filtered)" in html
        assert "</body>" in html

    def test_error_email_is_draftable(self, tmp_path, monkeypatch):
        """El correo de error (texto plano con traceback) también se puede
        guardar en .eml."""
        monkeypatch.setattr("smtplib.SMTP", lambda *a, **k: pytest.fail(
            "SMTP llamado"))
        sender = EmailSender("ab12345")
        path = sender.send(
            ["u1"], "Error in execution of source pnl",
            "An error was found in source pnl, contact data engineer team"
            "\nTraceback (most recent call last): ...",
            dry_run_dir=str(tmp_path))
        parsed = _parse_eml(path)
        assert "Error in execution of source pnl" in parsed["Subject"]
        assert "Traceback" in _html_body(parsed)
