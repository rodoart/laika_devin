# -*- coding: utf-8 -*-
"""
Project: Email Sender
Developer: Hector Morales
Update Date: 2021-03-05
Version: 1.0

Envío de correos (notificaciones del datapull) a través del SMTP del
clúster EAP.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
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

    def send(self, receivers_soeid: List[str], subject: str, message: str,
             file: Optional[str] = None) -> None:
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
        """
        receivers = ['{}@imcla.lac.nsroot.net'.format(soeid)\
                for soeid in receivers_soeid]

        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = ", ".join(receivers)
        msg['Subject'] = subject
        msg.attach(MIMEText(message, 'html'))

        if file:
            filename = os.path.basename(file)
            attachment = open(file, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

            msg.attach(part)

        server = smtplib.SMTP('mail.citicorp.com',25)
        text = msg.as_string()
        server.sendmail(self.email, receivers, text)
        server.quit()
