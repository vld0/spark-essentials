package com.vld0.spark;

import junit.framework.TestCase;
import org.junit.Test;

public class PageSizeTest extends TestCase {

    private PageSize pageSize = new PageSize();

    @Test
    public void test1() throws Exception {
        assertTrue(pageSize.call("http://mail.ru") > 0);
    }


}