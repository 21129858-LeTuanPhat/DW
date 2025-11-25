package org.example.email;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class EmailMessage {
    private String from;
    private String[] to;
    private String[] cc;
    private String[] bcc;
    private String subject;
    private String textContent;
    private String htmlContent;
    private List<File> attachments;

    public EmailMessage(String from, String[] to, String subject) {
        this.from = from;
        this.to = to;
        this.subject = subject;
        this.attachments = new ArrayList<>();
    }

    public static class Builder {
        private String from;
        private String[] to;
        private String subject;
        private String textContent;
        private String htmlContent;
        private List<File> attachments = new ArrayList<>();

        public Builder from(String from) {
            this.from = from;
            return this;
        }

        public Builder to(String... to) {
            this.to = to;
            return this;
        }

        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }

        public Builder text(String text) {
            this.textContent = text;
            return this;
        }

        public Builder html(String html) {
            this.htmlContent = html;
            return this;
        }

        public Builder addAttachment(File file) {
            this.attachments.add(file);
            return this;
        }

        public EmailMessage build() {
            EmailMessage message = new EmailMessage(from, to, subject);
            message.textContent = this.textContent;
            message.htmlContent = this.htmlContent;
            message.attachments = this.attachments;
            return message;
        }
    }

    public String getFrom() { return from; }
    public String[] getTo() { return to; }
    public String getSubject() { return subject; }
    public String getTextContent() { return textContent; }
    public String getHtmlContent() { return htmlContent; }
    public List<File> getAttachments() { return attachments; }
}
