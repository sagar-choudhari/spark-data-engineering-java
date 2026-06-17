package com.sagar.spark.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private static final Properties props = new Properties();

    static {
        try (InputStream in = AppConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")){
            props.load(in);
        } catch (IOException e) {
            throw  new RuntimeException("Failed to load application.properties", e);
        }
    }

    public static String get(String key){
        return props.getProperty(key);
    }
}

