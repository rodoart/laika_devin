# -*- coding: utf-8 -*-
"""
Project: Email Sender
Developer: Hector Morales
Update Date: 2021-03-05
Version: 1.0

"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path

class EmailSender:
    """
    Class for sending emails using EAP cluster
    """
    def __init__(self, sender_soeid) -> None:
        self.email = '{}@imcla.lac.nsroot.net'.format(sender_soeid)

    def send(self, receivers_soeid, subject, message, file=None) -> None:
        """
        This function sends an email from the sender email to the receivers.

        Parameters
        ----------
        receivers_soeid: List
            List containing the SOEID's of the persons that must
            receive the email
        subject: string
            The subject of the email
        message: string
            Message that the email will contain
        file: string
            Optional path that should be sent

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
