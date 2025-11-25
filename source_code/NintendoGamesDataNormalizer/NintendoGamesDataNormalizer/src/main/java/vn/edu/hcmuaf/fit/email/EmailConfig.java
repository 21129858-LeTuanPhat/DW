package vn.edu.hcmuaf.fit.email;

public class EmailConfig {
    private String host;
    private int port;
    private String username;
    private String password;
    private boolean enableTLS;

    public EmailConfig(String host, int port, String username,
                       String password, boolean enableTLS) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.enableTLS = enableTLS;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getUsername() { return username; }
    public String getPassword() { return password; }
    public boolean isEnableTLS() { return enableTLS; }
}
