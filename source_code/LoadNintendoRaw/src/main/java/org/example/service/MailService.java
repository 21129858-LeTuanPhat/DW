package org.example.service;



public class MailService {
    org.example.service.JavaMail javaMail;

    public MailService() {
        javaMail = new org.example.service.JavaMail();
    }


    public void sendMailTo(String to, String subject,String body) {
        javaMail.sendEmail(to, subject, body);
    }





}
