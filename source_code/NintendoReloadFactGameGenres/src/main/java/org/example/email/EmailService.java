package org.example.email;

import jakarta.mail.*;
import jakarta.mail.internet.*;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class EmailService {
    private final EmailConfig config;
    private final Session session;

    public EmailService(EmailConfig config) {
        this.config = config;
        this.session = createSession();
    }

    private Session createSession() {
        Properties props = new Properties();
        props.put("mail.smtp.host", config.getHost());
        props.put("mail.smtp.port", config.getPort());
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable",
                String.valueOf(config.isEnableTLS()));

        return Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(
                        config.getUsername(),
                        config.getPassword()
                );
            }
        });
    }

    public void sendEmail(EmailMessage emailMessage) throws MessagingException {
        Message message = new MimeMessage(session);

        // Set From
        message.setFrom(new InternetAddress(emailMessage.getFrom()));

        // Set To
        InternetAddress[] toAddresses = new InternetAddress[emailMessage.getTo().length];
        for (int i = 0; i < emailMessage.getTo().length; i++) {
            toAddresses[i] = new InternetAddress(emailMessage.getTo()[i]);
        }
        message.setRecipients(Message.RecipientType.TO, toAddresses);

        // Set Subject
        message.setSubject(emailMessage.getSubject());

        // Set Content (text hoặc HTML hoặc cả hai)
        if (emailMessage.getHtmlContent() != null &&
                emailMessage.getTextContent() != null) {
            // Multipart (text + HTML)
            MimeMultipart multipart = new MimeMultipart("alternative");

            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(emailMessage.getTextContent());

            MimeBodyPart htmlPart = new MimeBodyPart();
            htmlPart.setContent(emailMessage.getHtmlContent(), "text/html");

            multipart.addBodyPart(textPart);
            multipart.addBodyPart(htmlPart);

            message.setContent(multipart);
        } else if (emailMessage.getHtmlContent() != null) {
            message.setContent(emailMessage.getHtmlContent(), "text/html");
        } else {
            message.setText(emailMessage.getTextContent());
        }

        // Gửi email
        Transport.send(message);
    }

    public void sendEmailWithAttachments(EmailMessage emailMessage)
            throws MessagingException, IOException {
        Message message = new MimeMessage(session);

        message.setFrom(new InternetAddress(emailMessage.getFrom()));
        message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(String.join(",", emailMessage.getTo())));
        message.setSubject(emailMessage.getSubject());

        // Tạo multipart để chứa body và attachments
        Multipart multipart = new MimeMultipart();

        // Thêm text/HTML body
        MimeBodyPart messageBodyPart = new MimeBodyPart();
        if (emailMessage.getHtmlContent() != null) {
            messageBodyPart.setContent(emailMessage.getHtmlContent(),
                    "text/html");
        } else {
            messageBodyPart.setText(emailMessage.getTextContent());
        }
        multipart.addBodyPart(messageBodyPart);

        // Thêm attachments
        for (File file : emailMessage.getAttachments()) {
            MimeBodyPart attachmentPart = new MimeBodyPart();
            attachmentPart.attachFile(file);
            multipart.addBodyPart(attachmentPart);
        }

        message.setContent(multipart);
        Transport.send(message);
    }
}
