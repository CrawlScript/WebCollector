/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.webcollector.util;

import org.webcollector.model.Page;
import static org.webcollector.util.HttpUtils.fetchHttpResponse;
import static org.webcollector.util.HttpUtils.fetchHttpResponse;

/**
 *
 * @author hu
 */
 class HttpRetry extends Retry<Page> {

        String url;
        ConnectionConfig conconfig;

        public HttpRetry(String url, ConnectionConfig conconfig) {
            this.url = url;
            this.conconfig = conconfig;
        }

        @Override
        public Page run() throws Exception {
            return fetchHttpResponse(url, conconfig);
        }

    }