package it.trevis.msauth;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("auth")
public class InitClassApp extends Application{
	public Set<Class<?>> getClasses() {
	       Set<Class<?>> s = new HashSet<Class<?>>();
	       s.add(AuthService.class);
	       return s;
	   }
}
