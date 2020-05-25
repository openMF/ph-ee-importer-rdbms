package hu.dpc.phee.operator.api;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.ExpressionUrlAuthorizationConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.config.annotation.web.configuration.EnableResourceServer;
import org.springframework.security.oauth2.config.annotation.web.configuration.ResourceServerConfigurerAdapter;
import org.springframework.security.oauth2.config.annotation.web.configurers.ResourceServerSecurityConfigurer;

@Configuration
@EnableResourceServer
public class ResourceServerConfig extends ResourceServerConfigurerAdapter {

    public static final String AUD_CLAIM_IN_TOKEN = "identity-provider";

    @Autowired(required = false)
    private AuthProperties properties;

    @Override
    public void configure(ResourceServerSecurityConfigurer resources) {
        resources.resourceId(AUD_CLAIM_IN_TOKEN)
                .stateless(true);
    }

    @Override
    public void configure(HttpSecurity http) throws Exception {
        if (properties != null) {
            ExpressionUrlAuthorizationConfigurer<HttpSecurity>.ExpressionInterceptUrlRegistry builder = http.sessionManagement()
                    .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                    .and()
                    .authorizeRequests();
            for (EndpointSetting s : properties.getSettings()) {
                builder.antMatchers(s.getEndpoint()).access(s.getAuthority());
            }
            builder.anyRequest().fullyAuthenticated().and()
                    .csrf().disable()
                    .anonymous().disable()
                    .cors();
        } else {
            http.authorizeRequests().anyRequest().permitAll();
        }
    }
}