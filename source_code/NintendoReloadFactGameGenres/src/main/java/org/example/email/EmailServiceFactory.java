package org.example.email;

public class EmailServiceFactory {

    public static EmailService createGmailService(String username,
                                                  String password) {
        EmailConfig config = new EmailConfig(
                "smtp.gmail.com",
                587,
                username,
                password,
                true
        );
        return new EmailService(config);
    }

    public static EmailService createMailtrapService(String username,
                                                     String password) {
        EmailConfig config = new EmailConfig(
                "sandbox.smtp.mailtrap.io",
                587,
                username,
                password,
                true
        );
        return new EmailService(config);
    }
}

