package org.example.email;

import jakarta.mail.MessagingException;

public class EmailHelper {
    private static final String EMAIL = "tahoangphuc1901@gmail.com";
    private static final String PASSWORD_APP = "acky hhik sjmo vxrh";

    private EmailHelper() {}

    public static void Send(String subject, String text, String html) {
        try {
            EmailService emailService = EmailServiceFactory
                    .createGmailService(EMAIL, PASSWORD_APP);

            EmailMessage message = new EmailMessage.Builder()
                    .from(EMAIL)
                    .to(EMAIL)
                    .subject(subject)
                    .text(text)
                    .html(html)
                    .build();

            emailService.sendEmail(message);
        } catch (MessagingException e) {
            System.out.println("Không thể gửi mail");
            throw new RuntimeException(e);
        }

    }
}
