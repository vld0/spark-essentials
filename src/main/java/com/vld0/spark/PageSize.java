package com.vld0.spark;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.function.MapFunction;

public class PageSize implements MapFunction<String, Long> {

    @Override
    public Long call(String url) throws Exception {
        HttpGet get = new HttpGet(url);

        HttpGet httpget = new HttpGet("http://localhost/");

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response = httpclient.execute(httpget);
        /*try {
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                InputStream instream = entity.getContent();
                try {            // do something useful        } finally {            instream.close();        }    }} finally {    response.close();}


                    return null;
                }
            }
        }
*/

        return -1l;
    }

}
