/*
 * Copyright (C) 2014 hu
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
package cn.edu.hfut.dmic.webcollector.souplang.nodes;

import cn.edu.hfut.dmic.webcollector.util.JDBCHelper;
import cn.edu.hfut.dmic.webcollector.souplang.Context;
import cn.edu.hfut.dmic.webcollector.souplang.LangNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 *
 * @author hu
 */
public class SLSQL extends LangNode {

    public static final Logger LOG = LoggerFactory.getLogger(SLSQL.class);
    public String template = null;
    public String sql = null;
    public String params = null;
    public String[] paramarray = null;

    public void readParams(org.w3c.dom.Element xmlElement) {
        params = xmlElement.getAttribute("params").trim();
        if (params.isEmpty()) {
            params = null;
        } else {
            paramarray = params.split(",");
        }
    }

    public void readSql(org.w3c.dom.Element xmlElement) {
        sql = xmlElement.getAttribute("sql");
        if (sql.isEmpty()) {
            sql = null;
        }
    }

    public void readTemplate(org.w3c.dom.Element xmlElement) {
        template = xmlElement.getAttribute("template");
        if (template.isEmpty()) {
            template = null;
        }
    }

    @Override
    public Object process(Object input, Context context) throws InputTypeErrorException {
        if (input == null) {
            return null;
        }
        if (sql == null || template == null) {
            return null;
        }

        JdbcTemplate jdbcTemplate = JDBCHelper.getJdbcTemplate(template);
        if (jdbcTemplate == null) {
            LOG.info("please create jdbctemplate");
            return null;
        }

        int paramsLength = paramarray.length;
        int updates;
        if (params !=null ) {
            String[] sqlParams = new String[paramsLength];
            for (int i = 0; i < paramsLength; i++) {
                sqlParams[i] = context.getString(paramarray[i]);
            }
            updates = jdbcTemplate.update(sql, sqlParams);

        } else {
            updates = jdbcTemplate.update(sql);
        }
        LOG.info(sql+params+"    result="+updates);
        return null;
    }

    @Override
    public boolean validate(Object input) throws Exception {
        return true;
    }

    
}
