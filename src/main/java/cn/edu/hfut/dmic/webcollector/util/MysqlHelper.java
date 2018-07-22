/*
 * Copyright (C) 2016 hu
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package cn.edu.hfut.dmic.webcollector.util;

import java.beans.PropertyVetoException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 *
 * @author hu
 */
public class MysqlHelper {

    DataSource dataSource;
    public JdbcTemplate template;

    public MysqlHelper(String url, String username, String password, int initialSize, int maxPoolSize) throws PropertyVetoException {
        dataSource = createDataSource(url,username,password,initialSize,maxPoolSize);
        template = new JdbcTemplate(dataSource);
     }

    public DataSource createDataSource(String url, String username, String password, int initialSize, int maxPoolSize) throws PropertyVetoException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("com.mysql.jdbc.Driver");
        dataSource.setJdbcUrl(url);
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setInitialPoolSize(initialSize);
        dataSource.setMaxPoolSize(maxPoolSize);
        dataSource.setTestConnectionOnCheckout(true);
        return dataSource;
    }

    public JdbcTemplate getTemplate() {
        return template;
    }

    public void setTemplate(JdbcTemplate template) {
        this.template = template;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
