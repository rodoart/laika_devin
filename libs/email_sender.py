# -*- coding: utf-8 -*-
"""
Project: Email Sender
Developer: Hector Morales
Update Date: 2021-03-05
Version: 1.0

Envío de correos (notificaciones del datapull) a través del SMTP del
clúster EAP.

Simulacro local: pasando ``dry_run_dir`` a :meth:`EmailSender.send` o con la
variable ``LAIKA_EMAIL_DRY_RUN_DIR`` el correo no se envía — se escribe el
mensaje RFC 822 completo en ``<dry_run_dir>/<ts>_<subject>.eml`` para poder
inspeccionarlo (tests, ``--local``).
"""
import os.path
import re
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional


class EmailSender:
    """Envío de correos usando el clúster EAP.

    Parameters
    ----------
    sender_soeid : str
        SOEID del remitente; el email se construye como
        ``<soeid>@imcla.lac.nsroot.net``.
    """
    def __init__(self, sender_soeid: str) -> None:
        self.email: str = '{}@imcla.lac.nsroot.net'.format(sender_soeid)

    def _receivers(self, receivers_soeid: List[str]) -> List[str]:
        """SOEIDs -> direcciones completas ``@imcla.lac.nsroot.net``."""
        return ['{}@imcla.lac.nsroot.net'.format(soeid)
                for soeid in receivers_soeid]

    def build_message(self, receivers_soeid: List[str], subject: str,
                      message: str,
                      file: Optional[str] = None) -> MIMEMultipart:
        """Construye el ``MIMEMultipart`` completo (testeable sin SMTP).

        Parameters
        ----------
        receivers_soeid : List[str]
            Lista de SOEIDs de las personas que deben recibir el correo.
        subject : str
            Asunto del correo.
        message : str
            Cuerpo del correo (HTML).
        file : Optional[str]
            Path opcional de un adjunto a enviar.

        Returns
        -------
        MIMEMultipart
            Mensaje con ``From``/``To``/``Subject``, el body HTML y el
            adjunto en base64 si se pidió.
        """
        receivers = self._receivers(receivers_soeid)

        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = ", ".join(receivers)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'html'))

        if file:
            filename = os.path.basename(file)
            with open(file, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                "attachment; filename= %s" % filename)
            msg.attach(part)

        return msg

    @staticmethod
    def _save_draft(msg: MIMEMultipart, dry_run_dir: str) -> str:
        """Escribe el mensaje en ``<dry_run_dir>/<ts>_<subject>.eml``."""
        os.makedirs(dry_run_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        safe_subject = re.sub(
            r'[^A-Za-z0-9_.-]+', '_', msg.get('Subject') or 'mail')[:60]
        path = os.path.join(dry_run_dir, f"{ts}_{safe_subject}.eml")
        with open(path, "w", encoding="utf-8") as draft:
            draft.write(msg.as_string())
        return path

    def send(self, receivers_soeid: List[str], subject: str, message: str,
             file: Optional[str] = None,
             dry_run_dir: Optional[str] = None) -> Optional[str]:
        """Envía un correo desde el remitente a los receptores.

        Parameters
        ----------
        receivers_soeid : List[str]
            Lista de SOEIDs de las personas que deben recibir el correo.
        subject : str
            Asunto del correo.
        message : str
            Cuerpo del correo (HTML).
        file : Optional[str]
            Path opcional de un adjunto a enviar.
        dry_run_dir : Optional[str]
            Simulacro: si se pasa (o existe ``LAIKA_EMAIL_DRY_RUN_DIR``) no
            se abre conexión SMTP — el mensaje se guarda en
            ``<dir>/<ts>_<subject>.eml`` y se devuelve su path.

        Returns
        -------
        Optional[str]
            Path del ``.eml`` escrito en modo simulacro; ``None`` en envío
            real.
        """
        msg = self.build_message(receivers_soeid, subject, message, file)
        #
        dry_run_dir = dry_run_dir or os.environ.get("LAIKA_EMAIL_DRY_RUN_DIR")
        if dry_run_dir:
            return self._save_draft(msg, dry_run_dir)
        #
        receivers = self._receivers(receivers_soeid)
        server = smtplib.SMTP('mail.citicorp.com',25)
        text = msg.as_string()
        server.sendmail(self.email, receivers, text)
        server.quit()
        return None
