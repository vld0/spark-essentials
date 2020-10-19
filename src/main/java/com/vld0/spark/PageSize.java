package com.vld0.spark;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.api.java.function.MapFunction;

import java.io.IOException;

public class PageSize implements MapFunction<String, Long> {

    @Override
    public Long call(String url) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        try (CloseableHttpResponse response = httpclient.execute(get)) {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                byte[] content = IOUtils.toByteArray(entity.getContent());
                //System.out.println(Strings.getString(content, Charset.defaultCharset().toString()));
                return (long)content.length;
            }
        } catch (IOException e) {
            return -1l;
        }

        return -1l;
    }

}
