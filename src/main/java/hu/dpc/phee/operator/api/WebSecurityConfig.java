package hu.dpc.phee.operator.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.WebSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
@EnableWebSecurity
public class WebSecurityConfig extends WebSecurityConfigurerAdapter {

    @Value("${rest.authorization.enabled:false}")
    private boolean isSecurityEnabled;

    public void configure(WebSecurity webSecurity) {
        if (isSecurityEnabled) {
            webSecurity.ignoring().antMatchers("/");
        } else {
            webSecurity.ignoring().antMatchers("/**");
        }
    }
}
