package com.snowflake.kafka.connector;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.connect.errors.ConnectException;

public enum CompressionType {
    NONE("none", ""),
    GZIP("gzip", ".gz") {
        public OutputStream wrapForOutput(OutputStream out) {
            try {
                return new GZIPOutputStream(out, 8192);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        public InputStream wrapForInput(InputStream in) {
            try {
                return new GZIPInputStream(in);
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        }

        public void finalize(OutputStream compressionFilter) {
            if (compressionFilter instanceof DeflaterOutputStream) {
                try {
                    ((DeflaterOutputStream) compressionFilter).finish();
                } catch (Exception e) {
                    throw new ConnectException(e);
                }
            } else {
                throw new ConnectException("Expected compressionFilter to be a DeflatorOutputStream, but was passed an instance that does not match that type.");
            }
        }
    };

    private static final int GZIP_BUFFER_SIZE_BYTES = 8192;

    public final String name;

    public final String extension;

    CompressionType(String name, String extension) {
        this.name = name;
        this.extension = extension;
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        if (GZIP.name.equals(name))
            return GZIP;
        throw new IllegalArgumentException("Unknown compression name: " + name);
    }

    public OutputStream wrapForOutput(OutputStream out) {
        return out;
    }

    public InputStream wrapForInput(InputStream in) {
        return in;
    }

    public void finalize(OutputStream compressionFilter) {
    }
}
