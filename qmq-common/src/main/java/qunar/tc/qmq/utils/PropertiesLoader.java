/*
 * Copyright (c) 2012 Qunar.com. All Rights Reserved.
 */
package qunar.tc.qmq.utils;

import com.google.common.io.Closer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-7
 */
public class PropertiesLoader {
    public static Properties load(String fileName, Properties parent) {
        final Closer closer = Closer.create();

        final InputStream is = PropertiesLoader.class.getClassLoader().getResourceAsStream(fileName);
        if (is == null)
            return null;

        try {
            final BufferedReader reader = closer.register(new BufferedReader(new InputStreamReader(is, "UTF-8")));
            final Properties prop = new Properties(parent);
            prop.load(reader);
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            qunar.agile.Closer.close(closer);
        }
    }
}
